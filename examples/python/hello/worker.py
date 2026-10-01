from hello import hello
from workers import WorkerEntrypoint, Response


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        body = await request.json()
        name = body.get("name", "Peren")
        print(f"python handled {name}")
        return Response(
            hello(self.env.GREETING, name),
            status=202,
            headers={"content-type": "text/plain; charset=utf-8", "x-runtime": "python"},
        )
