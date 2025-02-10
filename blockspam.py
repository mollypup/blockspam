from atproto import Client, models
import atproto_client
from time import sleep
import datetime
import httpx


def main():
    user, pwd = 'user', 'pwd'
    repo = resolve_handle(user)
    pds = resolve_pds(repo)
    session = None
    client = create_client(pds, user, pwd, session)

    with open("plc_all.csv") as f:
        data = f.read()

    dids = [d[1:-1] for d in data.splitlines()]
    day_1 = dids[:1_000_000]
    spam_blocks(client, day_1, repo)


def create_client(pds, user, pwd, session=None):
    client = Client(pds)
    if session is not None:
        profile = client.login(session_string=session)
    else:
        profile = client.login(user, pwd)
        print(client.export_session_string())

    print('Welcome,', profile.display_name)
    return client


def spam_blocks(client, dids, repo):
    did_split = split_list(dids, 200)
    print("Spamming!")
    for i, d in enumerate(did_split):
        created_at = client.get_current_time_iso()
        list_items = (models.AppBskyGraphBlock.Record(
            created_at=created_at,
            subject=did
        ) for did in d)

        list_of_writes = [
            models.com.atproto.repo.apply_writes.Create(
                collection="app.bsky.graph.block",
                value=l_i
            )
            for l_i in list_items
        ]

        for attempt in [3, 7, 14, 24, 40, 62, 100]:
            try:
                client.com.atproto.repo.apply_writes(
                    data=models.com.atproto.repo.apply_writes.Data(
                        repo=repo,
                        writes=list_of_writes
                    )
                )
            except atproto_client.exceptions.InvokeTimeoutError:
                if attempt == 100:
                    raise Exception("try again buddy")
                print("timeout error. sleeping")
                sleep(attempt)
                print("done eeping")
            else:
                sleep(120)  # sleep to be nice to firehouse-chan
                break
        print(f"spammed! {i}")


def unix_to_iso_string(timestamp: float | int):
    """
    Returns JavaScript-like timestamp strings
    e.g. 2000-01-01T00:00:00.000Z
    """
    return (
        datetime.datetime.fromtimestamp(timestamp).isoformat(
            timespec="milliseconds"
        )
        + "Z"
    )


def resolve_pds(did):
    if did.startswith("did:plc:"):
        r = httpx.get(f"https://plc.directory/{did}")
        r.raise_for_status()
    elif did.startswith("did:web"):
        r = httpx.get(f"https://{did.lstrip("did:web")}/.well-known/did.json")
        r.raise_for_status()
    else:
        raise ValueError("Invalid DID Method")
    for service in r.json()["service"]:
        if service["id"] == "#atproto_pds":
            return service["serviceEndpoint"]


def resolve_handle(user):
    if user.startswith("did:"):
        did = user
    else:
        pub = Client("https://public.api.bsky.app")
        did = pub.resolve_handle(user).did

    return did


def split_list(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]


if __name__ == '__main__':
    main()
