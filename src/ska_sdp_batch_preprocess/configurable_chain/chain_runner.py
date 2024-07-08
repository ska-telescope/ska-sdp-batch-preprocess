import yaml
import ska_sdp_func_python.preprocessing
import os



def chain_runner(data, config):
    """
    function to run a chain of processing functions 
    set out in a yaml configuration file.
    """
    with open(config, 'r') as f:
        chain = yaml.full_load(f)

    for func in chain['processing_functions'].keys():
        for params in chain['processing_functions'][func]:
            if type(chain['processing_functions'][func][params]) is list:
                chain['processing_functions'][func][params] = [float(i) for i in chain['processing_functions'][func][params]]
        print(func)
        func(data, chain['processing_functions'][func])
    
    return data
