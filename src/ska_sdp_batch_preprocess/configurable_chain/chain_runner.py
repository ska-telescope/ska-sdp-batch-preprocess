import yaml
from ska_sdp_func_python.preprocessing import apply_rfi_masks, averaging_frequency
import os
import numpy



def chain_runner(data, config):
    """
    function to run a chain of processing functions 
    set out in a yaml configuration file.
    """
    with open(config, 'r') as f:
        chain = yaml.full_load(f)

    for func in chain['processing_functions'].keys():
        for params in chain['processing_functions'][func]:
            if isinstance(chain['processing_functions'][func][params], list):
                x = numpy.array(chain['processing_functions'][func][params]).astype(numpy.float32)
                chain['processing_functions'][func][params] = x
                
        arguments = chain['processing_functions'][func]
        data = eval(func)(data, **arguments)
    
    return data
