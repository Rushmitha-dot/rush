"""The whole nightly job for one year, first version against tuned, writing all three marts to scratch."""
import shutil

from .. import summarize
from . import common


def main(argv=None):
    args = common.parse_args("job", argv)
    spark, cfg, owned = common.session("job")
    scratch = cfg.scratch / "job"

    def make(name, opts):
        def go(s):
            out = summarize.write_marts(s, cfg, args.year, opts, out_root=scratch / name)
            return out["rows"]
        return go

    variants = [
        common.Variant("baseline", make("baseline", summarize.BASELINE),
                       note="AQE off, sort merge joins, Python UDF, no cache, 200 files per mart"),
        common.Variant("tuned", make("tuned", summarize.TUNED),
                       note="AQE on, broadcast joins, native functions, cache, 1 file per mart"),
    ]
    common.run("job", variants, args, spark, cfg)
    shutil.rmtree(scratch, ignore_errors=True)
    common.finish(spark, owned)


if __name__ == "__main__":
    main()
