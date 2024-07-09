import chain_runner
from ska_sdp_datamodels.visibility import create_visibility_from_ms

measurementset = '~/MS_for_tests/lba2.MS'
data = create_visibility_from_ms(measurementset)

mychain = chain_runner.chain_runner(data[0], 'conf_example.yaml')
           