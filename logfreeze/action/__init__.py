def test_input_connect(appconfig, input):
    from logfreeze.input import test_connect

    inputconfig = [ic for ic in appconfig.inputs if ic.name == input][0]
    test_connect(inputconfig)


def test_sink_connect(appconfig, sink):
    from logfreeze.sink import test_connect

    sinkconfig = [ic for ic in appconfig.sinks if ic.name == sink][0]
    test_connect(sinkconfig)
