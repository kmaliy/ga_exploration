"""Reference SQL shipped with the package.

Read these with :mod:`importlib.resources` so they resolve inside a wheel or a
container image rather than depending on the working directory::

    from importlib.resources import files

    ddl = files("ga_pipeline.sql").joinpath("ddl.sql").read_text()
"""
