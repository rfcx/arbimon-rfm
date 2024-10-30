import numpy as np
from scipy.signal import firwin
from scipy.signal import resample_poly

from fractions import gcd

# Creates an FIR filter for audio resampling
def resample_poly_filter_window(up, down, beta=5.0, L=16001):
    "Constructs a resampling window for use with resample_poly_filter"
    # *** this block STOLEN FROM scipy.signal.resample_poly ***
    # Determine our up and down factors
    # Use a rational approximation to save computation time on really long
    # signals
    g_ = gcd(up, down)
    up //= g_
    down //= g_
    max_rate = max(up, down)

    sfact = np.sqrt(1+(beta/np.pi)**2)

    # generate first filter attempt: with 6dB attenuation at f_c
    filt = firwin(L, 1.0/max_rate, window=('kaiser', beta))

    N_FFT = 2**19
    NBINS = N_FFT/2+1
    paddedfilt = np.zeros(N_FFT)
    paddedfilt[:L] = filt
    ffilt = np.fft.rfft(paddedfilt)

    # now find the minimum between f_c and f_c+sqrt(1+(beta/pi)^2)/L
    bot = int(np.floor(NBINS/max_rate))
    top = int(np.ceil(NBINS*(1.0/max_rate + 2.0*sfact/L)))
    firstnull = (np.argmin(np.abs(ffilt[bot:top])) + bot)/NBINS

    # generate the proper shifted filter
    filt2 = firwin(L, -firstnull+2.0/max_rate, window=('kaiser', beta))

    return filt2


def resample_poly_filter(data, current_sample_rate, new_sample_rate):
    "Resamples a 1d array from a current_sample_rate to a new_sample_rate using resample_poly_filter"
    
    if new_sample_rate in (current_sample_rate*2, current_sample_rate/2):
        new_sample_rate+=1

		# Create a filter for resampling
    window = resample_poly_filter_window(new_sample_rate, current_sample_rate)
    # Resampling with polyphase filtering
    data = resample_poly(data, new_sample_rate, current_sample_rate, window=window)

    return data
