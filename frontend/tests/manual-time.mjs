export class ManualScheduler {
  nextId = 1;
  timers = new Map();

  setTimeout(callback, delay) {
    const id = this.nextId++;
    this.timers.set(id, { callback, delay });
    return id;
  }

  clearTimeout(id) {
    this.timers.delete(id);
  }

  pendingDelays() {
    return Array.from(this.timers.values(), (timer) => timer.delay);
  }

  runAll() {
    const timers = Array.from(this.timers.values());
    this.timers.clear();
    for (const timer of timers) timer.callback();
  }
}
