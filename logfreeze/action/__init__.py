from logfreeze import input
from logfreeze import sink


def test_input_connect(appconfig, input_name):
    inputconfig = [ic for ic in appconfig.inputs if ic.name == input_name][0]
    input.test_connect(inputconfig)


def test_input_dev(appconfig, input_name):
    inputconfig = [ic for ic in appconfig.inputs if ic.name == input_name][0]
    input.test_dev(inputconfig)


def test_input_timings(appconfig, input_name):
    inputconfig = [ic for ic in appconfig.inputs if ic.name == input_name][0]
    input.test_timings(inputconfig)


def test_sink_connect(appconfig, sink_name):
    sinkconfig = [ic for ic in appconfig.sinks if ic.name == sink_name][0]
    sink.test_connect(sinkconfig)
