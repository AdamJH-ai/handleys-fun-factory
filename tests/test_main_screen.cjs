const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createSequenceController } = require('../static/js/main_screen_sequences.js');

class Clock {
    now = 0;
    nextId = 1;
    timers = new Map();
    setTimeout(callback, milliseconds) {
        const id = this.nextId++;
        this.timers.set(id, { callback, at: this.now + milliseconds });
        return id;
    }
    clearTimeout(id) { this.timers.delete(id); }
    tick(milliseconds) {
        const end = this.now + milliseconds;
        for (;;) {
            const next = [...this.timers].filter(([, task]) => task.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
            if (!next) break;
            const [id, task] = next;
            this.now = task.at;
            this.timers.delete(id);
            task.callback();
        }
        this.now = end;
    }
}
class Audio extends EventTarget {
    constructor(mode = 'ok') { super(); this.mode = mode; this.muted = false; this.plays = 0; this.paused = true; }
    play() {
        this.plays++;
        this.paused = false;
        if (this.mode === 'throw') throw new Error('playback unavailable');
        return this.mode === 'reject' ? Promise.reject(new Error('autoplay blocked')) : Promise.resolve();
    }
    pause() { this.paused = true; }
    end() { this.dispatchEvent(new Event('ended')); }
    fail() { this.dispatchEvent(new Event('error')); }
}
const flush = () => new Promise(resolve => setImmediate(resolve));

for (const failure of ['reject', 'throw', 'error', 'stall', 'missing']) {
    test(`audio ${failure} finishes once and cancels outstanding callbacks`, async () => {
        const clock = new Clock();
        const controller = createSequenceController(clock);
        let completed = 0;
        const sequence = controller.start(() => completed++);
        const audio = failure === 'missing' ? null : new Audio(failure);
        sequence.play(audio, sequence.finish);
        if (failure === 'error') audio.fail();
        if (failure === 'stall') clock.tick(90000);
        await flush();
        assert.equal(completed, 1);
        if (audio) { audio.fail(); audio.end(); assert.equal(audio.paused, true); }
        clock.tick(300000);
        assert.equal(completed, 1);
        assert.equal(clock.timers.size, 0);
    });
}

test('cancel on disconnect prevents old media and timers completing a later sequence', async () => {
    const clock = new Clock();
    const controller = createSequenceController(clock);
    let oldComplete = 0, newComplete = 0, oldDelay = 0;
    const first = controller.start(() => oldComplete++);
    const oldAudio = new Audio('reject');
    first.play(oldAudio, first.finish);
    first.delay(() => oldDelay++, 100);
    controller.cancel();
    const second = controller.start(() => newComplete++);
    const newAudio = new Audio();
    second.play(newAudio, second.finish);
    oldAudio.end();
    await flush();
    clock.tick(100);
    assert.equal(oldComplete + oldDelay + newComplete, 0);
    newAudio.end();
    assert.equal(newComplete, 1);
    assert.equal(clock.timers.size, 0);
});

test('normal multi-step sequence plays each clip and completes exactly once', () => {
    const clock = new Clock();
    const controller = createSequenceController(clock);
    let completed = 0;
    const first = new Audio(), last = new Audio();
    const sequence = controller.start(() => completed++);
    sequence.play(first, () => sequence.delay(() => sequence.play(last, sequence.finish), 1000));
    first.end(); first.end();
    clock.tick(1000);
    assert.equal(last.plays, 1);
    assert.equal(completed, 0);
    last.end(); last.end();
    assert.equal(completed, 1);
    assert.equal(clock.timers.size, 0);
});

function mainScreen() {
    const html = fs.readFileSync(path.join(__dirname, '../templates/main_screen.html'), 'utf8');
    const source = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(match => match[1]).join('\n');
    const callbacks = {}, emits = [], button = { disabled: false };
    const clock = new Clock();
    const media = Object.fromEntries(['themeMusic', 'announcerAudio', 'hostWelcomeAudio', 'victoryMusicAudio', 'hostCongratsAudio', 'hostExplainerAudio'].map(name => [name, new Audio()]));
    const context = vm.createContext({
        window: { location: { hostname: 'localhost' } },
        io: () => ({ on(name, callback) { callbacks[name] = callback; }, emit(...args) { emits.push(args); } }),
        createSequenceController: () => createSequenceController(clock),
        document: { addEventListener() {}, getElementById() { return button; }, querySelectorAll() { return Object.values(media); } },
        console: { log() {}, warn() {}, error() {}, trace() {} },
        media,
    });
    vm.runInContext(source, context);
    vm.runInContext(Object.keys(media).map(name => `${name} = media.${name};`).join('\n'), context);
    return { callbacks, emits, clock, media, button };
}

for (const [event, response] of [['start_game_intro_sequence', 'game_intro_finished'], ['show_how_to_play', 'how_to_play_finished']]) {
    test(`${event}: restoration acknowledges the matching token once`, () => {
        const screen = mainScreen();
        screen.callbacks[event]({ sequence_id: 'current-token', restored: true });
        screen.clock.tick(300000);
        assert.equal(screen.emits.length, 1);
        assert.equal(screen.emits[0][0], response);
        assert.equal(screen.emits[0][1].sequence_id, 'current-token');
    });
}

test('intro audio rejection signals server without waiting forever', async () => {
    const screen = mainScreen();
    screen.media.themeMusic.mode = 'reject';
    screen.callbacks.start_game_intro_sequence({ sequence_id: 'intro-token' });
    await flush();
    screen.clock.tick(300000);
    assert.equal(screen.emits.length, 1);
    assert.equal(screen.emits[0][0], 'game_intro_finished');
    assert.equal(screen.emits[0][1].sequence_id, 'intro-token');
    assert.equal(screen.media.announcerAudio.plays, 0);
});

test('normal intro still waits 37 seconds, then announcer and host', () => {
    const screen = mainScreen();
    screen.callbacks.start_game_intro_sequence({ sequence_id: 'intro' });
    screen.clock.tick(36999);
    assert.equal(screen.media.announcerAudio.plays, 0);
    screen.clock.tick(1);
    assert.equal(screen.media.announcerAudio.plays, 1);
    screen.media.announcerAudio.end();
    screen.clock.tick(1000);
    assert.equal(screen.media.hostWelcomeAudio.plays, 1);
    screen.media.hostWelcomeAudio.end();
    assert.equal(screen.emits.length, 1);
    assert.equal(screen.emits[0][0], 'game_intro_finished');
});

for (const failure of ['reject', 'stall', 'restored', 'muted']) {
    test(`finale ${failure} enables Play Again with a working handler`, async () => {
        const screen = mainScreen();
        screen.media.victoryMusicAudio.mode = failure;
        screen.media.themeMusic.muted = failure === 'muted';
        screen.callbacks.start_game_over_sequence({ sequence_id: 'finale', restored: failure === 'restored' });
        await flush();
        screen.clock.tick(300000);
        assert.equal(screen.button.disabled, false);
        screen.button.onclick();
        assert.equal(screen.emits.length, 1);
        assert.equal(screen.emits[0][0], 'request_reset_game');
    });
}

test('explainer media error advances once with token', () => {
    const screen = mainScreen();
    screen.callbacks.show_how_to_play({ sequence_id: 'explainer', audio_file: 'example.mp3' });
    screen.media.hostExplainerAudio.fail();
    screen.media.hostExplainerAudio.end();
    assert.equal(screen.emits.length, 1);
    assert.equal(screen.emits[0][0], 'how_to_play_finished');
    assert.equal(screen.emits[0][1].sequence_id, 'explainer');
});

test('both HTML entry points have valid JavaScript syntax', () => {
    for (const name of ['index.html', 'main_screen.html']) {
        const html = fs.readFileSync(path.join(__dirname, '../templates', name), 'utf8');
        for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
    }
});

test('synchronous intro failure leaves no delayed announcer or timers', () => {
    const screen = mainScreen();
    screen.media.themeMusic.mode = 'throw';
    screen.callbacks.start_game_intro_sequence({ sequence_id: 'intro-token' });
    assert.equal(screen.emits.length, 1);
    assert.equal(screen.clock.timers.size, 0);
    screen.clock.tick(300000);
    assert.equal(screen.media.announcerAudio.plays, 0);
});
