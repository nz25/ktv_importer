# main.py

from prepare_data import main as prepare_data
from load_data import main as load_data
from process_open import main as process_open
from create_outputs import main as create_outputs

prepare_data()
load_data()
process_open()
create_outputs()


