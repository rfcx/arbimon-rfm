from pylab import *

class Plotter:
        
    def show(self , spec):    
        ax1 = subplot(111)
        im = ax1.imshow(spec, None)
        ax1.axis('auto')
        show()
        close()