from pyflink.datastream import StreamExecutionEnvironment


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    # print(env.get_parallelism())

    env.read_text_file("data/input/word_count.txt") \
        .flat_map(lambda x: str(x).split(",")) \
        .filter(lambda x: x) \
        .map(lambda x: str(x).strip()) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .sum(1) \
        .print()
    env.execute('WordCountLambda')

