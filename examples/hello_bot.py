"""Connection demonstration, not a trading idea. Uses simulated money only."""

import os
from uuid import uuid4

from simex.client import APIError, Client, StreamGap


def main():
    url = f"http://127.0.0.1:{os.environ['EXCHANGE_API_PORT']}"
    with Client(url, os.environ["EXCHANGE_API_TOKEN"]) as client:
        print("Quote:", client.quote("BTC-USD"))
        with client.stream() as stream:  # Receive hello before submitting.
            if stream.hello["writer_halted"] or stream.hello["account_halted"]:
                raise SystemExit("Account/writer halted; inspect the exchange before trading")
            # Minimum BTC size; taker fee is 120 bps (1.2%) of simulated notional, ADR 0014.
            order = client.place_order(
                client_order_id=uuid4().hex,
                product_id="BTC-USD",
                side="buy",
                type="market",
                quantity="0.00001",
                reasoning="Connection demonstration, not a trading idea",
            )["order"]
            print("Accepted:", order["order_id"])
            for frame in stream:
                if frame["type"] == "halt" and frame["halted"]:
                    raise SystemExit(f"Trading halted: {frame['scope']}")
                if frame["type"] == "fill":
                    own_order = frame["fill"]["order_id"] == order["order_id"]
                    if own_order:
                        print("Fill (including fee):", frame["fill"])
                    stream.ack(frame)  # After processing, including ignoring unrelated orders.
                    if own_order:
                        print("Portfolio:", client.portfolio())
                        break  # Demonstrate the first fill; a market remainder cannot rest.


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except (APIError, StreamGap) as error:
        raise SystemExit(str(error)) from None
