from workers import WorkerEntrypoint, Response


class Default(WorkerEntrypoint):
    def fetch(self, request):
        print("python fetch")
        return Response.json({"runtime": "python", "ok": True})

    def scheduled(self, event):
        print(f"python scheduled {event.cron} at {event.scheduledTime}")

    def tail(self, events):
        print(f"python tail received {len(events)} event(s)")
