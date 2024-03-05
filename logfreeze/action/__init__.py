from logfreeze import input
from logfreeze import sink
from logfreeze import task


def runtask(appconfig, task_name):
    taskconfig = appconfig.tasks[task_name]  # XXX

    input_name, sink_name = taskconfig['input'], taskconfig['sink']
    inputconfig = [ic for ic in appconfig.inputs if ic.name == input_name][0]
    sinkconfig = [ic for ic in appconfig.sinks if ic.name == sink_name][0]

    assert taskconfig.get('action') == 'filterforward'
    task.filterforward(inputconfig, sinkconfig, taskconfig['filters'])


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
