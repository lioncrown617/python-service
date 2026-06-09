import os
import json
import time
import math
import threading
import queue
from datetime import datetime
from collections import defaultdict, deque

import requests
from flask import Flask, jsonify, request, send_file, render_template, Response, stream_with_context

app = Flask(__name__)

NODE_API  = os.environ.get('NODE_API',  'http://localhost:3000/odds')
NODE_PUSH = os.environ.get('NODE_PUSH', 'http://localhost:3000/push')

def _deque10(): return deque(maxlen=10)
def _deque60(): return deque(maxlen=60)
def _inf(): return float('inf')

# ── safe division（新增）──────────────────────────────────────────────────────
def _sdiv(a, b): return a / b if b else 0.0

def parse_pool(pool_str):
    try:
        return float(str(pool_str).replace('$','').replace(',','').strip())
    except:
        return 0.0

def fmt_money(amt):
    a = abs(amt)
    if a >= 1_000_000: return f'${a/1_000_000:.2f}M'
    if a >= 1_000:     return f'${a/1_000:.1f}K'
    return f'${a:.0f}'

VENUE_NAME_MAP = {
    'ST': '沙田', 'HV': '跑馬地',
    **{f'S{i}': f'特別賽事 S{i}' for i in range(1, 9)},
}

TREND_THRESHOLD   = 2
ACCEL_DROP_MIN    = 2
WIN_BIG_THRESHOLD = 10000
Q_BIG_THRESHOLD   = 10000
MCI_W_WIN = 1.0
MCI_W_QIN = 0.6
MCI_W_QPL = 0.4
monitor_thread = None

_sse_clients = set()
_sse_lock    = threading.Lock()

def _broadcast_flask_sse(payload_str):
    dead = set()
    with _sse_lock:
        clients = set(_sse_clients)
    for q in clients:
        try:
            q.put_nowait(payload_str)
        except:
            dead.add(q)
    if dead:
        with _sse_lock:
            _sse_clients.difference_update(dead)

@app.route('/stream')
def flask_stream():
    q = queue.Queue(maxsize=20)
    with _sse_lock:
        _sse_clients.add(q)
    def generate():
        try:
            while True:
                try:
                    msg = q.get(timeout=20)
                    yield f'data: {msg}\n\n'
                except queue.Empty:
                    yield ': hb\n\n'
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                _sse_clients.discard(q)
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'}
    )

state = {
    'running': False, 'data': [], 'base_data': {}, 'base_time': '',
    'base_est_bet': {}, 'prev_data': {}, 'prev_est_bet': {}, 'prev_flow': {},
    'prev_pool': 0.0, 'prev_odds_drop': {}, 'trend_counter': defaultdict(int),
    'cum_drop': defaultdict(float), 'cum_rise': defaultdict(float), 'cum_flow': defaultdict(float),
    'update_count': 0, 'last_update': '', 'race_date': '', 'venue': '', 'venue_name': '',
    'race_no': '', 'interval': 1, 'current_interval': 1, 'url': '', 'status': '等待設定...',
    'has_error': False, 'top_down': [], 'top_up': [], 'top_acc': [], 'alerts': [],
    'history': defaultdict(list), 'bet_history': defaultdict(list), 'flow_history': defaultdict(list),
    'absorb_history': defaultdict(list), 'sms_history': defaultdict(list), 'acc_history': defaultdict(list),
    'timestamps': [], 'win_pool': '', 'win_pool_history': [],
    '_accels': {}, '_absorb': {}, '_sms': {}, '_alerts': {},
    'e_history': defaultdict(_deque10), 'inflow_ts_history': defaultdict(_deque60),
    'min_odds': defaultdict(_inf), 'alert_cooldown': defaultdict(dict),
    'steady_scores': {}, 'last_error_detail': '', 'race_info': {},
    'qin_flow': {}, 'qpl_flow': {}, 'qin_history': [], 'qpl_history': [],
    'last_upd_at': '', 'win_big_history': [],
    'win_big_cum': defaultdict(float),
    'qin_big_cum': defaultdict(float),
    'qpl_big_cum': defaultdict(float),
    'mci_list': [],
}

def fetch_odds_api(date_str, venue, race_no):
    try:
        resp = requests.get(NODE_API, params={'date': date_str, 'venue': venue, 'raceno': race_no}, timeout=4)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('ok'):
            state['last_error_detail'] = data.get('error', 'Node API 錯誤')
            return None, '', {}, ''
        results  = data.get('results', [])
        win_pool = data.get('win_pool', '')
        upd_at   = data.get('updAt', '')
        race_info = {
            'racetime':  data.get('race_time', ''),
            'distance':  data.get('distance', ''),
            'track':     data.get('track', ''),
            'course':    data.get('course', ''),
            'raceclass': data.get('race_class', ''),
            'going':     data.get('going', ''),
            'prize':     data.get('prize', ''),
            'racename':  data.get('race_name', ''),
        }
        if not results:
            state['last_error_detail'] = '無賽馬數據'
            return None, '', {}, ''
        return results, win_pool, race_info, upd_at
    except Exception as e:
        state['last_error_detail'] = str(e)
        return None, '', {}, ''

def calc_est_bets(data, pool_str):
    real_map = {}; has_real = False
    for r in data:
        amt = float(r.get('win_investment', 0) or 0)
        real_map[r['no']] = amt
        if amt > 0: has_real = True
    if has_real: return real_map
    pool_num  = parse_pool(pool_str)
    net_pool  = pool_num * (1 - 0.175)
    total_inv = sum(1.0 / float(r['win']) for r in data if r['win'] not in ('', 'SCR'))
    result = {}
    for r in data:
        try:
            share = (1.0 / float(r['win'])) / total_inv if total_inv > 0 else 0
            result[r['no']] = net_pool * share
        except:
            result[r['no']] = 0.0
    return result

def calc_trends(data):
    prev = state['prev_data']; base = state['base_data']
    tc = state['trend_counter']; cd = state['cum_drop']; cr = state['cum_rise']
    for r in data:
        no = r['no']
        try:
            curr = float(r['win'])
            if curr < state['min_odds'][no]: state['min_odds'][no] = curr
            if no in prev:
                diff = curr - float(prev[no])
                if diff < 0:   tc[no] = tc[no] + 1 if tc[no] > 0 else 1
                elif diff > 0: tc[no] = tc[no] - 1 if tc[no] < 0 else -1
            if no in base:
                base_w = float(base[no]); pct = (base_w - curr) / base_w * 100
                if pct > 0: cd[no] = round(pct, 1); cr[no] = 0.0
                else:       cr[no] = round(abs(pct), 1); cd[no] = 0.0
        except: pass

def calc_sms_v2(no, cum_flow_val, cum_drop_val):
    F = max(cum_flow_val / 10000.0, 0)
    if F <= 0: return 0.0
    D = max(cum_drop_val, 0)
    e_hist = list(state['e_history'][no]); pos_e = [max(e, 0) for e in e_hist]
    E_eff  = sum(pos_e) / len(pos_e) if len(pos_e) >= 3 else 0.0
    now_ts = time.time(); hist = list(state['inflow_ts_history'][no])
    w_5  = sum(a for ts, a in hist if now_ts - ts <= 300)
    w_15 = sum(a for ts, a in hist if now_ts - ts <= 900)
    w_30 = sum(a for ts, a in hist if now_ts - ts <= 1800)
    r5  = w_5  / cum_flow_val if cum_flow_val > 0 else 0
    r15 = w_15 / cum_flow_val if cum_flow_val > 0 else 0
    r30 = w_30 / cum_flow_val if cum_flow_val > 0 else 0
    if r5 >= 0.50: Wt = 1.45
    elif r15 >= 0.45: Wt = 1.30
    elif r30 >= 0.38: Wt = 1.18
    else: Wt = 1.0
    rf = list(state['flow_history'][no])[-3:]
    if len(rf) >= 3 and all(f <= 0 for f in rf): Wt *= 0.55
    if len(rf) >= 3:
        al = [rf[i] - rf[i-1] for i in range(1, len(rf))]
        if all(f < 0 for f in rf) and all(a < 0 for a in al): Wt *= 0.75
    last6 = list(state['flow_history'][no])[-6:]
    stab  = 1.0 + (sum(1 for x in last6 if x > 0) / len(last6) if last6 else 0) * 0.12
    return round((F ** 1.2) * (1 + D/10) * (1 + E_eff/10) * Wt * stab, 2)

def calc_acc_score(no, cum_flow_val):
    ih = list(state['inflow_ts_history'][no])
    if len(ih) < 2: return 0.0
    be = [(ts, a) for ts, a in ih if a >= 10000]
    if len(be) < 2: return 0.0
    ts_span = (be[-1][0] - be[0][0]) / 60
    if ts_span < 1.0: return 0.0
    consistency = len(be) / max(len(ih), 1)
    F = max(cum_flow_val / 10000.0, 0)
    gaps = [(be[i][0] - be[i-1][0]) / 60 for i in range(1, len(be))]
    mgap = max(gaps) if gaps else ts_span
    gp = 1.0 if mgap <= 5 else 0.9 if mgap <= 10 else 0.75 if mgap <= 15 else 0.6
    bb = min(1.0 + math.log(max(len(be), 1), 2), 4.0)
    db = min(max(ts_span / 8.0, 0.0), 1.2)
    return round((F ** 1.08) * consistency * gp * bb * (1.0 + db * 0.15), 2)

def calc_acc_meta(no):
    ih = list(state['inflow_ts_history'][no])
    be = [(ts, a) for ts, a in ih if a >= 10000]
    if len(be) < 2: return {'batch_count': 0, 'time_span': 0.0, 'max_gap_min': 0.0}
    ts_span = round((be[-1][0] - be[0][0]) / 60, 1)
    gaps = [(be[i][0] - be[i-1][0]) / 60 for i in range(1, len(be))]
    return {'batch_count': len(be), 'time_span': ts_span, 'max_gap_min': round(max(gaps), 1) if gaps else ts_span}

def calc_flow_and_signals(est_bets, win_pool_str, data):
    prev_bets = state['prev_est_bet']; prev_fl = state['prev_flow']
    prev_pool = state['prev_pool'];    prev_drop = state['prev_odds_drop']
    cum_flow  = state['cum_flow'];     cum_drop_pct = state['cum_drop']
    now_ts = time.time()
    now_str = datetime.now().strftime('%H:%M:%S')
    curr_pool_num = parse_pool(win_pool_str)
    pool_increase = max((curr_pool_num - prev_pool) * (1 - 0.175), 0)
    total_inv = sum(1.0 / float(r['win']) for r in data if r['win'] not in ('', 'SCR'))
    flows = {}; accels = {}; absorbs = {}; sms = {}; alerts = {}
    name_map = {r['no']: r.get('name','') for r in data}
    for r in data:
        no = r['no']; win_str = r['win']
        if win_str in ('', 'SCR'): continue
        try: curr_odds = float(win_str)
        except: continue
        amt = est_bets.get(no, 0.0); prev_amt = prev_bets.get(no, None)
        flow  = 0.0 if prev_amt is None else amt - prev_amt
        accel = 0.0 if prev_fl.get(no) is None else flow - prev_fl[no]
        if prev_amt is not None and flow > 0:
            cum_flow[no] = cum_flow.get(no, 0.0) + flow
            state['inflow_ts_history'][no].append((now_ts, flow))
        try: share_pct = (1.0 / curr_odds) / total_inv * 100 if total_inv > 0 else 0
        except: share_pct = 0.0
        absorb_pct = 0.0; excess = 0.0
        if pool_increase > 500 and prev_amt is not None:
            absorb_pct = (flow / pool_increase) * 100
            excess = absorb_pct - share_pct
            state['e_history'][no].append(excess)
        prev_o = float(state['prev_data'].get(no, curr_odds) or curr_odds)
        try: odds_drop = (prev_o - curr_odds) / prev_o * 100 if prev_o > 0 else 0.0
        except: odds_drop = 0.0
        odds_accel = odds_drop - prev_drop.get(no, 0.0)
        sms_score  = calc_sms_v2(no, cum_flow.get(no, 0.0), cum_drop_pct.get(no, 0.0))
        acc_score  = calc_acc_score(no, cum_flow.get(no, 0.0))
        alert_flags = []
        if state['trend_counter'].get(no, 0) >= ACCEL_DROP_MIN and odds_accel > 0.5:
            alert_flags.append(f'⚡賠率加速跌({odds_drop:.1f}%)')
        if prev_amt is not None and flow >= WIN_BIG_THRESHOLD:
            alert_flags.append(f'💥突發大注{fmt_money(flow)}')
            state['win_big_history'] = ([{
                'time': now_str, 'no': no,
                'name': name_map.get(no, no),
                'flow': flow, 'raw': flow, 'odds': win_str,
            }] + state['win_big_history'])[:100]
            state['win_big_cum'][no] = state['win_big_cum'].get(no, 0.0) + flow
        try:
            min_o = state['min_odds'].get(no, curr_odds)
            rfm = (curr_odds - min_o) / min_o * 100 if min_o > 0 else 0
            if rfm > 50 and cum_flow.get(no, 0) > 100000:
                alert_flags.append(f'🔔疑似洗碼受益(反彈{rfm:.0f}%)')
        except: pass
        rf3 = list(state['flow_history'][no])[-3:]
        if len(rf3) >= 3:
            al = [rf3[i] - rf3[i-1] for i in range(1, len(rf3))]
            if all(f < 0 for f in rf3) and all(a < 0 for a in al):
                alert_flags.append('🌊資金退潮警告')
        flows[no] = flow; accels[no] = accel
        absorbs[no] = {
            'flow': round(flow), 'absorb_pct': round(absorb_pct,1), 'share_pct': round(share_pct,1),
            'excess': round(excess,1), 'pool_inc': round(pool_increase), 'odds_drop': round(odds_drop,2),
            'odds_accel': round(odds_accel,2), 'is_rescue': False,
        }
        sms[no] = sms_score; sms[f'acc_{no}'] = acc_score; alerts[no] = alert_flags
    return flows, accels, absorbs, sms, alerts

def _update_qin_qpl_background():
    try:
        qin_api = NODE_API.replace('/odds','/qin-qpl')
        resp = requests.get(qin_api, params={
            'date': state['race_date'], 'venue': state['venue'], 'raceno': state['race_no']
        }, timeout=3)
        resp.raise_for_status(); data = resp.json()
        if not data.get('ok'): return
        prev_qin = state['qin_flow']; prev_qpl = state['qpl_flow']
        now_str = datetime.now().strftime('%H:%M:%S')
        def process(pool_data, prev_map, history_key, big_cum_key):
            entries = []; curr_map = {}
            for item in pool_data.get('odds', []):
                combo = item['combo']
                try: curr_inv = float(item.get('investment', 0) or 0)
                except: curr_inv = 0.0
                curr_map[combo] = curr_inv
                prev_inv = prev_map.get(combo)
                if prev_inv is not None:
                    flow = curr_inv - prev_inv
                    if flow >= 1000:
                        entries.append({'time': now_str, 'combo': combo.replace('+','-'),
                            'odds': item.get('odds',''), 'flow': flow, 'raw': flow})
                    if flow >= Q_BIG_THRESHOLD:
                        for part in combo.replace('+','/').replace('-','/').split('/'):
                            no = part.strip()
                            if no:
                                state[big_cum_key][no] = state[big_cum_key].get(no, 0.0) + flow
            state[history_key] = (entries + state[history_key])[:200]
            return curr_map
        state['qin_flow'] = process(data.get('qin',{}), prev_qin, 'qin_history', 'qin_big_cum')
        state['qpl_flow'] = process(data.get('qpl',{}), prev_qpl, 'qpl_history', 'qpl_big_cum')
        _calc_mci()
    except: pass

# ════════════════════════════════════════════════════════════════════════════
# _calc_mci — 正規化跨彩池大注信心指數（改版）
# 公式：
#   norm_win = win_big_i / P_win_net          (WIN 大注佔淨彩池份額)
#   norm_qin = (qin_big_i / combo_div) / P_qin_total  (QIN 修正組合倍數後份額)
#   norm_qpl = (qpl_big_i / combo_div) / P_qpl_total
#   raw_i    = 1.0*norm_win + 0.6*norm_qin + 0.4*norm_qpl
#   MCI_i    = raw_i / max(raw) * 100
# ════════════════════════════════════════════════════════════════════════════
def _calc_mci():
    data = state['data']
    if not data: return

    # 活躍馬匹數（排除 SCR）
    n = sum(1 for r in data if r.get('win') not in ('', 'SCR', None))
    combo_div = max(n - 1, 1)   # QIN/QPL 每匹馬最多出現在 (n-1) 個組合

    # 各彩池分母
    P_win = parse_pool(state['win_pool']) * (1 - 0.175)  # 淨 WIN 彩池
    P_qin = sum(state['qin_big_cum'].values()) or 1.0    # 全場 QIN 大注總量
    P_qpl = sum(state['qpl_big_cum'].values()) or 1.0    # 全場 QPL 大注總量

    raw_scores = {}
    for r in data:
        no = r['no']
        if r.get('win') in ('', 'SCR', None): continue
        w = state['win_big_cum'].get(no, 0.0)
        q = state['qin_big_cum'].get(no, 0.0)
        p = state['qpl_big_cum'].get(no, 0.0)
        if w + q + p <= 0: continue   # 無大注，不入榜

        norm_win = _sdiv(w, P_win)
        norm_qin = _sdiv(_sdiv(q, combo_div), P_qin)
        norm_qpl = _sdiv(_sdiv(p, combo_div), P_qpl)
        raw_scores[no] = MCI_W_WIN * norm_win + MCI_W_QIN * norm_qin + MCI_W_QPL * norm_qpl

    if not raw_scores:
        state['mci_list'] = []
        return

    max_val = max(raw_scores.values()) or 1.0
    result  = []
    for r in data:
        no = r['no']
        if no not in raw_scores: continue
        w = state['win_big_cum'].get(no, 0.0)
        q = state['qin_big_cum'].get(no, 0.0)
        p = state['qpl_big_cum'].get(no, 0.0)
        score = round(_sdiv(raw_scores[no], max_val) * 100, 1)
        result.append({
            'no':      no,
            'name':    r.get('name', ''),
            'win':     r.get('win', ''),
            'mci':     score,
            # 各彩池份額百分比（顯示用）
            'win_pct': round(_sdiv(w, P_win) * 100, 2),
            'qin_pct': round(_sdiv(_sdiv(q, combo_div), P_qin) * 100, 2),
            'qpl_pct': round(_sdiv(_sdiv(p, combo_div), P_qpl) * 100, 2),
            # 原始大注金額
            'win_big': round(w),
            'qin_big': round(q),
            'qpl_big': round(p),
            'has_win': w > 0,
            'has_qin': q > 0,
            'has_qpl': p > 0,
        })
    state['mci_list'] = sorted(result, key=lambda x: x['mci'], reverse=True)

def get_trend_label(no):
    tc = state['trend_counter']; prev = state['prev_data']
    dd = {r['no']: r for r in state['data']}; count = tc.get(no, 0)
    try:
        curr = float(dd[no]['win']); p = float(prev[no]) if no in prev else curr
        pct = (p - curr) / p * 100 if p > 0 else 0
    except: pct = 0
    if count >= TREND_THRESHOLD and pct >= 10: return '急跌','hot'
    elif count >= TREND_THRESHOLD and pct > 0: return '持跌','warm'
    elif count <= -TREND_THRESHOLD:            return '持升','rise'
    elif pct < 0:                              return '回升','rise'
    return '—','neutral'

def calc_top3():
    data=state['data']; base=state['base_data']; sms_map=state['_sms']
    absorbs=state['_absorb']; tc=state['trend_counter']; cd=state['cum_drop']; cum_f=state['cum_flow']
    sms_all = []
    for r in data:
        no=r['no']; ss=sms_map.get(no,0.0); ab=absorbs.get(no,{}); cum_in=cum_f.get(no,0.0)
        rf=list(state['flow_history'][no])[-3:]
        wk = len(rf)>=3 and all(f<=0 for f in rf)
        if ss>0 and tc.get(no,0)>=-1 and not wk:
            sms_all.append({'no':no,'name':r['name'],'win':r['win'],'base':base.get(no,'—'),
                'streak':max(tc.get(no,0),0),'drop':cd.get(no,0),'cum_inflow':round(cum_in),
                'sms':ss,'excess':ab.get('excess',0)})
    new_top = sorted(sms_all, key=lambda x: x['sms'], reverse=True)[:5]
    if new_top: state['top_down'] = new_top
    state['top_up'] = []
    acc_list = []
    for r in data:
        no=r['no']; cum_in=cum_f.get(no,0.0); acc_s=calc_acc_score(no,cum_in); meta=calc_acc_meta(no)
        if acc_s>=0 and meta['batch_count']>=1:
            acc_list.append({'no':no,'name':r['name'],'win':r['win'],'base':base.get(no,'—'),
                'cum_inflow':round(cum_in),'batch_count':meta['batch_count'],
                'time_span':meta['time_span'],'max_gap_min':meta['max_gap_min'],'acc':acc_s})
    new_acc = sorted(acc_list, key=lambda x: x['acc'], reverse=True)[:5]
    if new_acc: state['top_acc'] = new_acc

def update_global_alerts(alerts_map, now): pass

def record_history(data, now, est_bets, flows, absorbs, sms):
    state['timestamps'].append(now)
    for r in data:
        no = r['no']
        try: state['history'][no].append(float(r['win']))
        except: state['history'][no].append(None)
        state['bet_history'][no].append(round(est_bets.get(no,0)))
        state['flow_history'][no].append(round(flows.get(no,0)))
        state['absorb_history'][no].append(absorbs[no]['excess'] if no in absorbs else 0)
        state['sms_history'][no].append(sms.get(no,0))
        state['acc_history'][no].append(sms.get(f'acc_{no}',0))

def get_log_path():
    date_str = state['race_date'].replace('-','')
    os.makedirs('logs', exist_ok=True)
    return f"logs/{date_str}_{state['venue']}_R{str(state['race_no']).zfill(2)}_log.json"

def _load_log():
    path = get_log_path()
    if os.path.exists(path):
        try:
            with open(path,'r',encoding='utf-8') as f: return json.load(f)
        except: pass
    return {'meta':{},'snapshots':[],'alerts':[],'summary':{}}

def _save_log(log_data):
    try:
        with open(get_log_path(),'w',encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'[LOG ERROR] {e}')

def append_snapshot(now, data, est_bets, flows, absorbs, sms, win_pool):
    log = _load_log()
    if not log['meta']:
        log['meta'] = {'race_date':state['race_date'],'venue':state['venue'],'venue_name':state['venue_name'],
            'race_no':state['race_no'],'base_time':state['base_time'],'start_time':now,'race_info':state.get('race_info',{})}
    snapshot = {'time':now,'win_pool':win_pool,'horses':[]}
    for r in data:
        no=r['no']; ab=absorbs.get(no,{})
        snapshot['horses'].append({'no':r['no'],'name':r['name'],'win':r['win'],'place':r.get('place',''),
            'base_win':state['base_data'].get(no,'—'),'est_bet':round(est_bets.get(no,0)),
            'flow':round(flows.get(no,0)),'cum_flow':round(state['cum_flow'].get(no,0)),
            'absorb_pct':ab.get('absorb_pct',0),'excess':ab.get('excess',0),'pool_inc':ab.get('pool_inc',0),
            'odds_drop':ab.get('odds_drop',0),'cum_drop':state['cum_drop'].get(no,0),
            'cum_rise':state['cum_rise'].get(no,0),'sms':sms.get(no,0),'acc':sms.get(f'acc_{no}',0),
            'alerts':state['_alerts'].get(no,[])})
    log['snapshots'].append(snapshot); _save_log(log)

def finalize_log(now):
    log=_load_log(); cum_f=state['cum_flow']
    horses=[{'no':no,
        'name':next((r['name'] for r in state['data'] if r['no']==no),no),
        'final_win':next((r['win'] for r in state['data'] if r['no']==no),'—'),
        'cum_flow':round(v),'cum_drop':state['cum_drop'].get(no,0),
        'cum_rise':state['cum_rise'].get(no,0),'sms':state['_sms'].get(no,0),
        'acc':calc_acc_score(no,v)} for no,v in cum_f.items() if v>0]
    log['summary']={'end_time':now,'total_updates':state['update_count'],'base_time':state['base_time'],
        'final_pool':state['win_pool'],'total_alerts':0,
        'top_sms':sorted(horses,key=lambda x:x['sms'],reverse=True)[:5],
        'top_acc':sorted(horses,key=lambda x:x['acc'],reverse=True)[:5],'horses_final':horses}
    _save_log(log); print(f"[LOG] 已儲存：{get_log_path()}")

def _tier(history):
    return {
        'y':[x for x in history if x['raw']<50000][:100],
        'o':[x for x in history if 50000<=x['raw']<100000][:100],
        'r':[x for x in history if x['raw']>=100000][:100],
    }

def build_data_payload():
    pool_num  = parse_pool(state['win_pool']); net_pool = pool_num*(1-0.175)
    total_inv = sum(1.0/float(r['win']) for r in state['data'] if r['win'] not in ('','SCR'))
    rows=[]
    for r in state['data']:
        no=r['no']; label,css=get_trend_label(no); base_win=state['base_data'].get(no,'—')
        prev_win=state['prev_data'].get(no,''); chg_str=''
        try:
            diff=float(r['win'])-float(prev_win); pct=diff/float(prev_win)*100
            sign='+'if diff>0 else ''; chg_str=f'{sign}{diff:.1f}({sign}{pct:.1f}%)'
        except: pass
        estamt=0.0; estbet=''; estpct=''
        try:
            real_amt=float(r.get('win_investment',0) or 0)
            if real_amt>0: estamt=real_amt; estbet=fmt_money(estamt)
            else:
                share=(1.0/float(r['win']))/total_inv if total_inv>0 else 0
                estamt=net_pool*share; estbet=fmt_money(estamt); estpct=f'{share*100:.1f}%'
        except: pass
        base_amt=state['base_est_bet'].get(no,0.0); base_bet_str=fmt_money(base_amt) if base_amt>0 else ''
        cumdiff_str=''; cumdiff_css='neutral'; cumdiff_pct=''
        try:
            if base_amt>0:
                cdval=estamt-base_amt; sign='+'if cdval>0 else '-'
                cumdiff_str=f'{sign}{fmt_money(abs(cdval))}'; cumdiff_css='up'if cdval>0 else 'diluted'
                pv=cdval/base_amt*100; sign2='+'if pv>0 else ''; cumdiff_pct=f'{sign2}{pv:.1f}%'
        except: pass
        cum_in=state['cum_flow'].get(no,0.0); cum_in_str=fmt_money(cum_in) if cum_in>0 else ''
        ab=state['_absorb'].get(no,{}); flow=ab.get('flow',0); absorb_pct=ab.get('absorb_pct',0.0)
        excess=ab.get('excess',0.0); pool_inc=ab.get('pool_inc',0)
        odds_drop=ab.get('odds_drop',0.0); odds_accel=ab.get('odds_accel',0.0); is_rescue=ab.get('is_rescue',False)
        if flow>0:   flow_str,flow_css=f'+{fmt_money(flow)}{"🆘"if is_rescue else ""}','up'
        elif flow<0: flow_str,flow_css=f'-{fmt_money(abs(flow))}','diluted'
        else:        flow_str,flow_css='','neutral'
        accel=state['_accels'].get(no,0.0)
        if accel>=2000:  accel_str,accel_css=f'+{fmt_money(accel)}','hot'
        elif accel>0:    accel_str,accel_css=f'+{fmt_money(accel)}','up'
        elif accel<0:    accel_str,accel_css=f'-{fmt_money(abs(accel))}','diluted'
        else:            accel_str,accel_css='','neutral'
        if pool_inc>500 and absorb_pct!=0:
            if excess>=15:   absorb_str,absorb_css=f'{absorb_pct:.1f}%(+{excess:.1f}%)','hot'
            elif excess>=5:  absorb_str,absorb_css=f'{absorb_pct:.1f}%(+{excess:.1f}%)','up'
            elif excess>=0:  absorb_str,absorb_css=f'{absorb_pct:.1f}%','neutral'
            else:            absorb_str,absorb_css=f'{absorb_pct:.1f}%({excess:.1f}%)','diluted'
        else: absorb_str,absorb_css='','neutral'
        if odds_accel>1 and odds_drop>1: odrop_str,odrop_css=f'-{odds_drop:.1f}%','hot'
        elif odds_drop>0:  odrop_str,odrop_css=f'-{odds_drop:.1f}%','up'
        elif odds_drop<0:  odrop_str,odrop_css=f'+{abs(odds_drop):.1f}%','diluted'
        else:              odrop_str,odrop_css='','neutral'
        ss=state['_sms'].get(no,0.0); acc_s=state['_sms'].get(f'acc_{no}',0.0)
        if ss>=5:   sms_str,sms_css=f'{ss:.1f}','hot'
        elif ss>=1: sms_str,sms_css=f'{ss:.1f}','up'
        elif ss>0:  sms_str,sms_css=f'{ss:.1f}','neutral'
        else:       sms_str,sms_css='','neutral'
        try:
            curr_o=float(r['win']); min_o=state['min_odds'].get(no,curr_o)
            rfm=(curr_o-min_o)/min_o*100 if min_o>0 else 0
        except: rfm=0
        alert_str=' '.join(state['_alerts'].get(no,[]))
        rows.append({
            **{k:r.get(k,'') for k in ['no','name','barrier','jockey','trainer','win','place']},
            'basewin':base_win,'prevwin':prev_win,'chg':chg_str,'trend':label,'trendcss':css,
            'estbet':estbet,'estpct':estpct,'basebet':base_bet_str,'cumdiff':cumdiff_str,
            'cumdiffcss':cumdiff_css,'cumdiffpct':cumdiff_pct,'cuminflow':cum_in_str,
            'flow':flow_str,'flowcss':flow_css,'flowraw':round(flow),'accel':accel_str,
            'accelcss':accel_css,'absorb':absorb_str,'absorbcss':absorb_css,'odrop':odrop_str,
            'odropcss':odrop_css,'sms':sms_str,'smscss':sms_css,'smsraw':ss,'accraw':acc_s,
            'alert':alert_str,'issuspicious':False,'risefrommin':round(rfm,1),
        })
    winbig = state['win_big_history'][:20]
    return {
        'rows':rows, 'winbig':winbig, 'mcilist':state['mci_list'],
        'topdown':state['top_down'],'topup':state['top_up'],'topacc':state['top_acc'],
        'alerts':[],'updatecount':state['update_count'],'lastupdate':state['last_update'],
        'basetime':state['base_time'],'status':state['status'],'haserror':state['has_error'],
        'running':state['running'],'racedate':state['race_date'],'venuename':state['venue_name'],
        'raceno':state['race_no'],'interval':state['interval'],'currentinterval':state['current_interval'],
        'history':{k:v for k,v in state['history'].items()},
        'bethistory':{k:v for k,v in state['bet_history'].items()},
        'flowhistory':{k:v for k,v in state['flow_history'].items()},
        'absorbhistory':{k:v for k,v in state['absorb_history'].items()},
        'smshistory':{k:v for k,v in state['sms_history'].items()},
        'acchistory':{k:v for k,v in state['acc_history'].items()},
        'timestamps':state['timestamps'],'horses':{r['no']:r['name'] for r in state['data']},
        'winpool':state['win_pool'],'winpoolhistory':state['win_pool_history'],
        'errordetail':state.get('last_error_detail',''),'raceinfo':state.get('race_info',{}),
        'qin':_tier(state['qin_history']),'qpl':_tier(state['qpl_history']),
    }

def push_sse(payload):
    try:
        payload_str = json.dumps(payload, ensure_ascii=False)
        _broadcast_flask_sse(payload_str)
        requests.post(NODE_PUSH, data=payload_str,
                      headers={'Content-Type':'application/json'}, timeout=1)
    except: pass

def monitor_loop():
    state['status']='連接 Node.js API 中...'; state['has_error']=False
    while state['running']:
        now = datetime.now().strftime('%H:%M:%S')
        data, win_pool, race_info, upd_at = fetch_odds_api(state['race_date'], state['venue'], state['race_no'])
        if data:
            if upd_at and upd_at == state['last_upd_at']:
                time.sleep(0.5); continue
            state['last_upd_at'] = upd_at; state['has_error']=False; state['update_count']+=1
            if race_info: state['race_info']=race_info
            est_bets = calc_est_bets(data, win_pool)
            if not state['base_data']:
                state['base_data']={r['no']:r['win'] for r in data}
                state['base_time']=now; state['base_est_bet']=dict(est_bets)
            flows,accels,absorbs,sms,alerts_map = calc_flow_and_signals(est_bets, win_pool, data)
            calc_trends(data)
            record_history(data, now, est_bets, flows, absorbs, sms)
            append_snapshot(now, data, est_bets, flows, absorbs, sms, win_pool)
            update_global_alerts(alerts_map, now)
            state['data']=data; state['prev_data']={r['no']:r['win'] for r in data}
            state['prev_odds_drop']={no:absorbs[no]['odds_drop'] for no in absorbs}
            state['last_update']=now; state['win_pool']=win_pool
            state['win_pool_history'].append({'time':now,'pool':win_pool})
            state['prev_flow']=flows; state['prev_est_bet']=dict(est_bets)
            state['prev_pool']=parse_pool(win_pool); state['_accels']=accels
            state['_absorb']=absorbs; state['_sms']=sms; state['_alerts']=alerts_map
            calc_top3(); _calc_mci()
            threading.Thread(target=_update_qin_qpl_background, daemon=True).start()
            iv=state['interval']; state['current_interval']=iv
            state['status']=f'✅ 正常監察中 · {iv}s'
            threading.Thread(target=push_sse, args=(build_data_payload(),), daemon=True).start()
        else:
            state['has_error']=True
            state['status']=f"[{now}] 連接失敗 | {state.get('last_error_detail','')[:80]}"
        time.sleep(state['interval'])
    finalize_log(datetime.now().strftime('%H:%M:%S'))
    state['status']='監察已停止'; state['has_error']=False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start():
    global monitor_thread
    if state['running']: return jsonify({'ok':False,'msg':'已在監察中'})
    d = request.json or {}
    state['race_date']  = d.get('date', datetime.now().strftime('%Y-%m-%d'))
    state['venue']      = d.get('venue','ST')
    state['venue_name'] = VENUE_NAME_MAP.get(state['venue'], state['venue'])
    state['race_no']    = d.get('raceno','1')
    state['interval']   = max(int(d.get('interval',1)),1)
    state['url'] = f"https://bet.hkjc.com/ch/racing/wp/{state['race_date']}/{state['venue']}/{state['race_no']}"
    for k,v in [
        ('running',True),('has_error',False),('data',[]),('base_data',{}),('base_est_bet',{}),('prev_data',{}),
        ('prev_est_bet',{}),('prev_flow',{}),('prev_pool',0.0),('prev_odds_drop',{}),('update_count',0),
        ('top_down',[]),('top_up',[]),('top_acc',[]),('alerts',[]),('timestamps',[]),('win_pool',''),
        ('win_pool_history',[]),('_accels',{}),('_absorb',{}),('_sms',{}),('_alerts',{}),('steady_scores',{}),
        ('current_interval',1),('status','正在啟動...'),('last_error_detail',''),('race_info',{}),
        ('qin_flow',{}),('qpl_flow',{}),('qin_history',[]),('qpl_history',[]),('last_upd_at',''),
        ('win_big_history',[]),('mci_list',[]),
    ]: state[k]=v
    state['trend_counter']=defaultdict(int); state['cum_drop']=defaultdict(float)
    state['cum_rise']=defaultdict(float);    state['cum_flow']=defaultdict(float)
    state['history']=defaultdict(list);      state['bet_history']=defaultdict(list)
    state['flow_history']=defaultdict(list); state['absorb_history']=defaultdict(list)
    state['sms_history']=defaultdict(list);  state['acc_history']=defaultdict(list)
    state['e_history']=defaultdict(_deque10);state['inflow_ts_history']=defaultdict(_deque60)
    state['min_odds']=defaultdict(_inf);     state['alert_cooldown']=defaultdict(dict)
    state['win_big_cum']=defaultdict(float)
    state['qin_big_cum']=defaultdict(float)
    state['qpl_big_cum']=defaultdict(float)
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()
    return jsonify({'ok':True})

@app.route('/stop', methods=['POST'])
def stop():
    state['running']=False; return jsonify({'ok':True})

@app.route('/data')
def get_data():
    return jsonify(build_data_payload())

@app.route('/qinflow')
def get_qin_flow():
    if not state['race_date'] or not state['venue'] or not state['race_no']:
        return jsonify({'ok':False,'error':'監察未啟動'})
    try:
        qin_api = NODE_API.replace('/odds','/qin-qpl')
        resp = requests.get(qin_api, params={
            'date':state['race_date'],'venue':state['venue'],'raceno':state['race_no']
        }, timeout=4)
        resp.raise_for_status(); data=resp.json()
        if not data.get('ok'): return jsonify({'ok':False,'error':data.get('error','')})
        prev_qin=state['qin_flow']; prev_qpl=state['qpl_flow']
        now_str=datetime.now().strftime('%H:%M:%S')
        def process(pool_data, prev_map, history_key, big_cum_key):
            entries=[]; curr_map={}
            for item in pool_data.get('odds',[]):
                combo=item['combo']
                try: curr_inv=float(item.get('investment',0) or 0)
                except: curr_inv=0.0
                curr_map[combo]=curr_inv; prev_inv=prev_map.get(combo)
                if prev_inv is not None:
                    flow=curr_inv-prev_inv
                    if flow>=1000:
                        entries.append({'time':now_str,'combo':combo.replace('+','-'),
                            'odds':item.get('odds',''),'flow':flow,'raw':flow})
                    if flow >= Q_BIG_THRESHOLD:
                        for part in combo.replace('+','/').replace('-','/').split('/'):
                            no=part.strip()
                            if no: state[big_cum_key][no]=state[big_cum_key].get(no,0.0)+flow
            state[history_key]=(entries+state[history_key])[:200]; return curr_map
        state['qin_flow']=process(data.get('qin',{}),prev_qin,'qin_history','qin_big_cum')
        state['qpl_flow']=process(data.get('qpl',{}),prev_qpl,'qpl_history','qpl_big_cum')
        _calc_mci()
        return jsonify({'ok':True,'qin':_tier(state['qin_history']),'qpl':_tier(state['qpl_history'])})
    except Exception as e:
        return jsonify({'ok':False,'error':str(e)})

@app.route('/download_log')
def download_log():
    path=get_log_path()
    if os.path.exists(path): return send_file(path, as_attachment=True)
    return jsonify({'error':'Log 不存在，請先開始監察'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=False, host='0.0.0.0', port=port, threaded=True)
