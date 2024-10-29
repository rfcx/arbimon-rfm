import pytest
from rfm.old.process_rec import process_rec


def test_wav():
    rec = 'tests/data/wav_unknown/2022-11-01_11-25.wav'

    result = process_rec(rec, bin_size=344, threshold=0.005, frequency=100)

    assert result['recMaxHertz'] == 11025
