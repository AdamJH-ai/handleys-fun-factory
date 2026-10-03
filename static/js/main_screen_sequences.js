/* Audio steps must finish once, even after playback errors or a lost connection. */
(function (root) {
    'use strict';

    function createSequenceController(clock = globalThis) {
        let cancelCurrent = () => {};

        function start(onComplete, timeoutMs = 180000) {
            cancelCurrent();
            let active = true;
            const cleanups = [];
            function cancel() {
                if (!active) return;
                active = false;
                cleanups.splice(0).forEach(cleanup => cleanup());
            }
            function finish() {
                if (!active) return;
                cancel();
                onComplete();
            }
            function delay(callback, milliseconds) {
                if (!active) return;
                const timer = clock.setTimeout(() => {
                    if (active) callback();
                }, milliseconds);
                cleanups.push(() => clock.clearTimeout(timer));
            }
            function play(audio, onEnded = () => {}, { onError = finish, timeout = 90000 } = {}) {
                if (!active) return;
                if (!audio) { onError(); return; }
                let settled = false;
                let timer;
                const release = () => {
                    audio.removeEventListener('ended', ended);
                    audio.removeEventListener('error', failed);
                    clock.clearTimeout(timer);
                };
                function settle(callback) {
                    if (settled || !active) return;
                    settled = true;
                    release();
                    callback();
                }
                const ended = () => settle(onEnded);
                const failed = () => settle(onError);
                audio.addEventListener('ended', ended);
                audio.addEventListener('error', failed);
                timer = clock.setTimeout(failed, timeout);
                cleanups.push(() => { release(); audio.pause(); });
                try {
                    audio.currentTime = 0;
                    const promise = audio.play();
                    if (promise && promise.catch) promise.catch(failed);
                } catch (error) {
                    failed();
                }
            }
            cancelCurrent = cancel;
            delay(finish, timeoutMs);
            return { finish, delay, play };
        }
        return { start, cancel: () => cancelCurrent() };
    }

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = { createSequenceController };
    } else {
        root.createSequenceController = createSequenceController;
    }
})(globalThis);
