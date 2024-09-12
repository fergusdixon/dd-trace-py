import glob
import os
import time

from ddtrace.internal.datadog.profiling import ddup
from ddtrace.profiling.collector import stack
from tests.profiling.collector import pprof_utils


def func1():
    return func2()


def func2():
    return func3()


def func3():
    return func4()


def func4():
    return func5()


def func5():
    return time.sleep(1)


class TestStack:
    def setup_method(self, method):
        self.test_name = method.__name__
        self.pprof_prefix = "/tmp" + os.sep + self.test_name
        self.output_filename = self.pprof_prefix + "." + str(os.getpid())

        assert ddup.is_available, "ddup is not available"
        ddup.config(env="test", service=self.test_name, version="my_version", output_filename=self.pprof_prefix)
        ddup.start()
        pass

    def teardown_method(self, method):
        for f in glob.glob(self.output_filename + ".*"):
            try:
                os.remove(f)
            except Exception as e:
                print("Error removing file: {}".format(e))

    ########################### TESTS ###########################
    def test_collect_truncate(self):
        max_frames = 5
        with stack.StackCollector(None, nframes=max_frames):
            func1()
        ddup.upload()

        profile = pprof_utils.parse_profile(self.output_filename)
        sample = pprof_utils.get_sample_with_thread_name(profile, "MainThread")
        assert len(sample.location_id) == max_frames

        expected_function_names = ["func5", "func4", "func3", "func2", "func1"]
        for i, location_id in enumerate(sample.location_id):
            location = pprof_utils.get_location_with_id(profile, location_id)
            # We expect only one line element as we don't have inlined functions
            line = location.line[0]
            function = pprof_utils.get_function_with_id(profile, line.function_id)
            function_name = profile.string_table[function.name]
            assert function_name == expected_function_names[i]

    def test_collect_once(self):
        with stack.StackCollector(None):
            # Sleep for a while to ensure the collector has time to collect
            time.sleep(0.1)
        ddup.upload()

        profile = pprof_utils.parse_profile(self.output_filename)
        sample = pprof_utils.get_sample_with_thread_name(profile, "MainThread")

        thread_id = pprof_utils.get_thread_id(profile, sample)
        assert thread_id > 0
        assert len(sample.location_id) > 0
        location = pprof_utils.get_location_with_id(profile, sample.location_id[0])
        line = location.line[0]
        function = pprof_utils.get_function_with_id(profile, line.function_id)
        function_name = profile.string_table[function.name]
        assert function_name == "test_collect_once"

        class_name_label = pprof_utils.get_label_with_key(profile.string_table, sample, "class name")
        assert profile.string_table[class_name_label.str] == "TestStack"
