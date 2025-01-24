import time


class SnowflakeIDGenerator:
    def __init__(self, datacenter_id, worker_id):
        self.datacenter_id = datacenter_id
        self.worker_id = worker_id
        self.sequence = 0
        self.last_timestamp = -1

    def _current_timestamp(self):
        return int(time.time() * 1000)

    def generate(self):
        timestamp = self._current_timestamp()

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & 0xFFF
            if self.sequence == 0:
                timestamp = self._wait_for_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        snowflake_id = ((timestamp << 22) |
                        (self.datacenter_id << 17) |
                        (self.worker_id << 12) |
                        self.sequence)
        return snowflake_id

    def _wait_for_next_millis(self, last_timestamp):
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp


# 主程序
if __name__ == "__main__":
    generator = SnowflakeIDGenerator(datacenter_id=1, worker_id=1)
    snowflake_id = generator.generate()
    print(snowflake_id)
