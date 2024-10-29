import argparse

# TODO: Support args parser
def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    return parser.parse_args()

def read_config_from_args():
    return {}
