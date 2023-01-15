from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction


class MapClass(MapFunction):
    def map(self, value):
        return value, 1


def map_function(x: str):
    return x.strip()


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    # print(env.get_parallelism())

    env.read_text_file("data/input/word_count.txt") \
        .flat_map(lambda x: str(x).split(",")) \
        .filter(lambda x: x) \
        .map(map_function) \
        .map(MapClass()) \
        .key_by(lambda x: x[0]) \
        .sum(1) \
        .print()
    env.execute('WordCountFunction')

