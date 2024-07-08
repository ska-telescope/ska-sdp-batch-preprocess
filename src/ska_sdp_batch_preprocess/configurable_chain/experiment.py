import chain_runner
from ska_sdp_datamodels.visibility import create_visibility_from_ms

measurementset = '~/MS_for_tests/lba2.MS'
data = create_visibility_from_ms(measurementset)


mychain = chain_runner.chain_runner(data, 'conf_example.yaml')
for elems in mychain.keys():
    for member in mychain[elems].keys():
        for param in mychain[elems][member].keys():
            
            print(mychain[elems][member][param])
            print(type(mychain[elems][member][param]))
        
           