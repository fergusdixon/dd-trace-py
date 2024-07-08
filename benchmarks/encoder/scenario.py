from dataclasses import dataclass
from dataclasses import field

import bm
import utils


@dataclass
class EncoderParent:
    name: str
    ntraces: int
    nspans: int
    ntags: int
    ltags: int
    nmetrics: int
    encoding: str
    dd_origin: bool = field(default_factory=bm.var_bool)


class Encoder(EncoderParent, bm.Scenario):
    def run(self):
        encoder = utils.init_encoder(self.encoding)
        traces = utils.gen_traces(self)

        def _(loops):
            for _ in range(loops):
                for trace in traces:
                    encoder.put(trace)
                    encoder.encode()

        yield _
