import importlib.util
import sys
from pathlib import Path
import pytest
spec = importlib.util.spec_from_file_location('hff_test_app', Path(__file__).parents[1] / 'app.py')
g = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = g
spec.loader.exec_module(g)

@pytest.fixture
def game(monkeypatch):
    g.players.clear(); g.overall_game_scores.clear(); g.pid_to_sid.clear(); g.sid_to_pid.clear()
    g.game_state = 'waiting'; g.main_screen_sid = None
    g.wwts = g.SingingRound()
    monkeypatch.setattr(g, 'WWTS_FIRST_TEST', True)
    monkeypatch.setattr(g, 'wwts_target_turns', 5)
    monkeypatch.setattr(g.random, 'sample', lambda bank, count: bank[:count])
    scheduled = []
    monkeypatch.setattr(g.wwts, 'delay', lambda seconds, fn: scheduled.append(fn))
    return scheduled


def enter(n):
    main = g.socketio.test_client(g.app)
    main.emit('register_main_screen')
    phones = []
    for i in range(n):
        p = g.socketio.test_client(g.app)
        p.emit('register_player', {'name': f'Player {i}', 'pid': f'pid-{i}'})
        phones.append(p)
    g.current_game_round_num = 1; g.selected_rounds_for_game = ['win_when_singing']
    with g.app.app_context(): g.start_round_logic('win_when_singing')
    return main, phones


def start(main, phones, tasks):
    token = g.wwts.token
    for p in [main] + phones: p.emit('wwts_ready', {'token':token})
    assert g.wwts.phase == 'countdown'
    tasks.pop(0)()
    main.emit('wwts_started', {'token': g.wwts.token, 'start_ms':g.time.monotonic()*1000})
    assert g.wwts.phase == 'guess'
    # Move the origin into the past to test without sleeping for an entire song.
    g.wwts.start_ms -= 100000

@pytest.mark.parametrize('n', [2, 8])
def test_full_round(game, monkeypatch, n):
    main, phones = enter(n); start(main, phones, game)
    token = g.wwts.token
    differences = [-100, 100, 500, 501, -500, -501, 2000, -2000][:n]
    for p, delta in zip(phones, differences):
        p.emit('wwts_tap', {'token':token, 'tap_ms':g.wwts.start_ms+18210+delta})
    assert g.wwts.phase == 'replay'
    rows = g.wwts.rows
    assert rows[0]['points'] == rows[1]['points'] == 4
    if n == 8:
        assert [r['placing'] for r in rows] == [3,3,1,1,0,0,0,0]
        assert sum(r['bonus'] for r in rows) == 4
    score = [p['round_score'] for p in g.players.values()]
    phones[0].emit('wwts_tap', {'token':token,'tap_ms':g.wwts.start_ms+18210})
    assert score == [p['round_score'] for p in g.players.values()]
    reveal_token = g.wwts.token
    phones[0].emit('wwts_replay_done', {'token':reveal_token})
    assert g.wwts.phase == 'replay'
    main.emit('wwts_replay_done', {'token':reveal_token})
    main.emit('wwts_replay_done', {'token':reveal_token})
    assert len(game) == 1
    game.pop(0)()
    assert g.wwts.phase == 'summary'
    assert sum(g.overall_game_scores.values()) > 0
    awarded = dict(g.overall_game_scores)
    main.emit('wwts_replay_done', {'token':reveal_token})
    assert g.overall_game_scores == awarded
    monkeypatch.setattr(g, 'start_next_game_round', lambda: setattr(g, 'game_state', 'next_round'))
    game.pop(0)()
    assert g.game_state == 'next_round'


def test_private_and_invalid(game):
    main, phones = enter(2)
    for p in phones:
        prompt = [e for e in p.get_received() if e['name']=='wwts_prompt'][-1]['args'][0]
        assert '18.210' not in str(prompt) and 'track001' not in str(prompt) and 'Seven Nation' not in str(prompt)
    start(main, phones, game)
    for data in [None, {}, {'token':'old','tap_ms':1}, {'token':g.wwts.token,'tap_ms':True},
                 {'token':g.wwts.token,'tap_ms':float('nan')}, {'token':g.wwts.token,'tap_ms':g.wwts.start_ms-1},
                 {'token':g.wwts.token,'tap_ms':g.time.monotonic()*1000+10000}]:
        phones[0].emit('wwts_tap', data)
    assert not g.wwts.answers
    assert g.wwts.phase == 'guess'  # no answer timeout


def test_reconnect_preserves_locked_guess(game):
    main, phones = enter(2); start(main, phones, game)
    old = g.wwts.token
    phones[0].emit('wwts_tap', {'token':old,'tap_ms':g.wwts.start_ms+18000})
    phones[0].disconnect()
    assert g.wwts.phase == 'paused'
    new = g.socketio.test_client(g.app)
    new.emit('register_player', {'name':'Player 0','pid':'pid-0'})
    assert g.wwts.phase == 'prepare' and len(g.wwts.answers)==1
    start(main, [new, phones[1]], game)
    new.emit('wwts_tap', {'token':old,'tap_ms':g.wwts.start_ms+18210})
    assert list(g.wwts.answers.values()) == [18000]
    phones[1].emit('wwts_tap', {'token':g.wwts.token,'tap_ms':g.wwts.start_ms+18210})
    assert g.wwts.phase == 'replay'
    saved = [r['points'] for r in g.wwts.rows]
    main.disconnect()
    restored = g.socketio.test_client(g.app); restored.emit('register_main_screen')
    assert g.wwts.phase == 'replay' and [r['points'] for r in g.wwts.rows] == saved
    assert any(e['name']=='wwts_media' for e in restored.get_received())


def test_reset_invalidates_delays(game, monkeypatch):
    main, phones = enter(2)
    callbacks=[]
    monkeypatch.setattr(g.socketio, 'start_background_task', lambda f: callbacks.append(f))
    monkeypatch.setattr(g.socketio, 'sleep', lambda t: None)
    called=[]
    g.SingingRound.delay(g.wwts, 1, lambda: called.append(True))
    main.emit('request_reset_game')
    callbacks.pop()()
    assert not called and not g.wwts.live()


def test_data_validation(game, monkeypatch):
    assert g.wwts_timestamp('0:18:210')==18210
    for text in ['0:60:000', '18.210', '', None]:
        with pytest.raises(ValueError): g.wwts_timestamp(text)
    monkeypatch.setattr(g, 'WWTS_FIRST_TEST', False)
    main, phones = enter(2)
    assert g.wwts.phase == 'error' and 'Need 5 songs' in g.wwts.error


def test_two_songs_then_overall_game_over(game, monkeypatch):
    bank = g.load_wwts_songs()
    monkeypatch.setattr(g, 'load_wwts_songs', lambda: [dict(bank[0],id='a'),dict(bank[0],id='b')])
    monkeypatch.setattr(g, 'WWTS_FIRST_TEST', False)
    monkeypatch.setattr(g, 'wwts_target_turns', 2)
    monkeypatch.setattr(g.socketio, 'sleep', lambda _: None)
    main, phones = enter(2)
    for number in range(2):
        start(main, phones, game)
        for p,offset in zip(phones,[0,1000]):
            p.emit('wwts_tap', {'token':g.wwts.token,'tap_ms':g.wwts.start_ms+18210+offset})
        main.emit('wwts_replay_done', {'token':g.wwts.token})
        game.pop(0)()
    assert sorted(p['round_score'] for p in g.players.values()) == [4,8]
    assert g.wwts.phase == 'summary'
    game.pop(0)()
    assert g.game_state == 'overall_game_over'
    main.emit('request_reset_game')
    assert g.game_state == 'waiting' and not g.wwts.live()


def test_main_disconnect_during_guess(game):
    main, phones = enter(2); start(main, phones, game)
    main.disconnect()
    assert g.wwts.phase == 'paused'
    newmain=g.socketio.test_client(g.app); newmain.emit('register_main_screen')
    assert g.wwts.phase == 'prepare'


@pytest.mark.parametrize('reverse', [False, True])
@pytest.mark.parametrize('n', [2, 8])
def test_real_song_bank_two_orders(game, monkeypatch, reverse, n):
    monkeypatch.setattr(g, 'WWTS_FIRST_TEST', False)
    monkeypatch.setattr(g, 'wwts_target_turns', 2)
    monkeypatch.setattr(g.random, 'sample', lambda bank, count: list(reversed(bank))[:count] if reverse else bank[:count])
    monkeypatch.setattr(g.socketio, 'sleep', lambda _: None)
    main, phones = enter(n)
    played=[]
    for number in range(2):
        song = g.wwts.song
        played.append(song['audio_file'])
        prepare = [e['args'][0] for e in main.get_received() if e['name']=='wwts_media'][-1]
        assert prepare['url'] == '/static/'+song['audio_file']
        assert 'word' not in prepare
        start(main,phones,game)
        old_token = g.wwts.token
        for i,p in enumerate(phones):
            p.emit('wwts_tap', {'token':old_token,'tap_ms':g.wwts.start_ms+song['word_ms']+i*100})
        assert g.wwts.phase=='replay'
        assert g.wwts.rows[0]['delta']==0 and g.wwts.rows[0]['points']==4
        media=[e['args'][0] for e in main.get_received() if e['name']=='wwts_media'][-1]
        assert media['url']=='/static/'+song['audio_file']
        assert media['word']==song['word_ms']/1000
        main.emit('wwts_replay_done',{'token':g.wwts.token})
        game.pop(0)()
        if number == 0:
            assert g.wwts.phase=='prepare'
            phones[0].emit('wwts_tap', {'token':old_token,'tap_ms':g.time.monotonic()*1000})
            assert not g.wwts.answers
    assert len(set(played))==2
    assert g.wwts.phase=='summary'
    assert max(p['round_score'] for p in g.players.values())==8
    game.pop(0)()
    assert g.game_state=='overall_game_over'


def test_duplicate_audio_reports_both_songs(game, monkeypatch):
    original=g.Path.read_text
    def duplicate(path,*args,**kwargs):
        text=original(path,*args,**kwargs)
        if path.name=='win_when_singing.json':
            bank=g.json.loads(text)
            bank['songs'][1]['audio_file']=bank['songs'][0]['audio_file']
            return g.json.dumps(bank)
        return text
    monkeypatch.setattr(g.Path,'read_text',duplicate)
    with pytest.raises(ValueError,match='I Gotta Feeling and Seven Nation Army reference the same audio file'):
        g.load_wwts_songs()
    enter(2)
    assert g.wwts.phase=='error'
    assert 'track001.mp3' in g.wwts.error
