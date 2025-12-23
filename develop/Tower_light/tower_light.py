import asyncio
import json
import time
import nats
import psycopg2
from datetime import datetime
import os
import sys

# Load configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "config.json")

with open(config_path) as f:
    CONFIG = json.load(f)


class MachineMonitor:
    """Monitor database for a single machine's active notification count"""

    def __init__(self, machine_config, nc):
        self.machine_config = machine_config
        self.nc = nc
        self.connection_params = CONFIG["database"]
        self.last_state = None
        self.nats_topic = machine_config["nats_topic"]
        self.machine_id = machine_config["machine_id"]
        self.plc_id = machine_config["plc_id"]
        self.tag_name = machine_config["tag_name"]
        self.table_name = machine_config["table_name"]
        self.true_value = machine_config.get("true_value", True)
        self.false_value = machine_config.get("false_value", False)

    def get_connection(self):
        """Create database connection with timeout"""
        # Add connection timeout to prevent indefinite hangs
        params = {**self.connection_params, 'connect_timeout': 10}
        return psycopg2.connect(**params)

    def get_active_count(self):
        """Query database for machine active notification count (excluding TP01)"""
        try:
            print(f"[{self.machine_id}] DEBUG: Connecting to database...")
            conn = self.get_connection()
            print(f"[{self.machine_id}] DEBUG: Connected, creating cursor...")
            cur = conn.cursor()

            query = f"""
                SELECT machine_status
                FROM {self.table_name}
                WHERE machine_status->>'id' = %s;
            """

            print(f"[{self.machine_id}] DEBUG: Executing query on table {self.table_name}...")
            cur.execute(query, (self.machine_id,))
            print(f"[{self.machine_id}] DEBUG: Fetching result...")
            result = cur.fetchone()

            cur.close()
            conn.close()
            print(f"[{self.machine_id}] DEBUG: Query complete, result={result is not None}")

            if result is None:
                return None

            machine_status = result[0]  # JSON → dict

            # Get active_tp object (contains all active TPs)
            active_tp = machine_status.get("active_tp", {})

            # Count active TPs excluding TP01
            # If only TP01 is active, count should be 0
            active_count_excluding_tp01 = sum(
                1 for tp_id in active_tp.keys() if tp_id != "TP01"
            )

            return active_count_excluding_tp01

        except psycopg2.OperationalError as e:
            print(f"[{self.machine_id}] ❌ Database connection error: {e}")
            return None
        except Exception as e:
            print(f"[{self.machine_id}] ❌ Database error: {e}")
            return None

    async def send_plc_update(self, value):
        """Send UPDATE command to PLC via NATS"""
        try:
            message = {
                "plc": self.plc_id,
                "name": self.tag_name,
                "command": "UPDATE",
                "value": value,
            }

            # Send request and wait for response
            response = await self.nc.request(
                self.nats_topic, json.dumps(message).encode(), timeout=5.0
            )

            response_data = json.loads(response.data.decode())

            if response_data.get("ack"):
                return True
            elif "error" in response_data:
                print(f"[{self.machine_id}] ❌ PLC update error: {response_data['error']}")
                return False
            else:
                print(f"[{self.machine_id}] ⚠ Unexpected response: {response_data}")
                return False

        except asyncio.TimeoutError:
            print(f"[{self.machine_id}] ⏱ NATS request timeout")
            return False
        except Exception as e:
            print(f"[{self.machine_id}] ❌ Error sending NATS message: {e}")
            return False

    async def monitor(self):
        """Monitor this machine and send updates when state changes"""
        # Get active count from database
        active = self.get_active_count()

        if active is None:
            print(f"[{self.machine_id}] ⚠ Could not fetch active count")
            return

        # Determine desired PLC value based on active count
        # Use true_value if active > 0, otherwise use false_value
        desired_value = self.true_value if active > 0 else self.false_value

        # Only send update if state has changed
        if desired_value != self.last_state:
            print(
                f"[{self.machine_id}] ➡ State change: {self.last_state} → {desired_value} (active={active})"
            )
            success = await self.send_plc_update(desired_value)

            if success:
                self.last_state = desired_value
                print(f"[{self.machine_id}] ✅ PLC tag {self.tag_name} = {desired_value}")
            else:
                print(f"[{self.machine_id}] ❌ Failed to update PLC tag")
        else:
            print(
                f"[{self.machine_id}] ✓ No change (state={desired_value}, active={active})"
            )


class MultiMachineMonitor:
    """Monitor multiple machines concurrently"""

    def __init__(self):
        self.nc = None
        self.nats_server = CONFIG["nats"]["server"]
        self.polling_interval = CONFIG["polling_interval"]
        self.machines = []

    async def connect_nats(self):
        """Establish NATS connection"""
        try:
            self.nc = await nats.connect(self.nats_server)
            print(f"✅ Connected to NATS server: {self.nats_server}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to NATS: {e}")
            return False

    async def monitor_loop(self):
        """Main monitoring loop for all machines"""
        # Create monitor instances for each machine
        for machine_config in CONFIG["machines"]:
            monitor = MachineMonitor(machine_config, self.nc)
            self.machines.append(monitor)

        print(f"🚀 Starting multi-machine notification monitor...")
        print(f"   Machines: {', '.join([m.machine_id for m in self.machines])}")
        print(f"   Polling Interval: {self.polling_interval}s")
        print(f"\n   Machine-Topic Mapping:")
        for m in self.machines:
            print(f"     {m.machine_id:8} → {m.nats_topic}")
        print("=" * 70)

        while True:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"\n[{timestamp}] Polling all machines...")
                print("-" * 70)

                # Monitor all machines concurrently for 10x speed boost
                await asyncio.gather(*[machine.monitor() for machine in self.machines])

                print("-" * 70)

                # Wait before next poll
                await asyncio.sleep(self.polling_interval)

            except KeyboardInterrupt:
                print("\n⛔ Shutting down...")
                break
            except Exception as e:
                print(f"❌ Unexpected error in monitor loop: {e}")
                await asyncio.sleep(self.polling_interval)

    async def run(self):
        """Main entry point"""
        # Connect to NATS
        connected = await self.connect_nats()
        if not connected:
            print("❌ Failed to connect to NATS. Exiting...")
            return

        try:
            # Start monitoring
            await self.monitor_loop()
        finally:
            # Cleanup
            if self.nc:
                await self.nc.close()
                print("🔌 NATS connection closed")


async def main():
    """Application entry point"""
    monitor = MultiMachineMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        print("=" * 70)
        print("  Tower Light NATS Sender - v2.0 (Multi-Machine Support)")
        print("  Monitoring database and sending PLC updates via NATS")
        print("=" * 70)
        print()

        asyncio.run(main())

    except KeyboardInterrupt:
        print("\n👋 Program terminated by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
