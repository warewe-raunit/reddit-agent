import os
from dotenv import load_dotenv
import httpx

load_dotenv(override=True)


def main() -> None:
    key = os.getenv("OPENROUTER_API_KEY")
    if not key:
        print("OPENROUTER_API_KEY is not set")
        return

    response = httpx.get(
        "https://openrouter.ai/api/v1/auth/key",
        headers={"Authorization": f"Bearer {key}"},
        timeout=20,
    )
    print(f"OpenRouter key check status: {response.status_code}")
    print(response.text[:300])


if __name__ == "__main__":
    main()
