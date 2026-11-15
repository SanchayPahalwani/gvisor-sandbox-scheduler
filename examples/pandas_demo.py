"""Builds a small DataFrame and prints a groupby result.

Run it with: bin/gssctl run -f examples/pandas_demo.py
pandas is in the primed set (spec D15): code run with -c or -f runs in a fork of an
interpreter that has imported it already, so the import below costs nothing.
"""

import pandas as pd

runs = pd.DataFrame(
    {
        "language": ["python", "go", "python", "rust", "go", "python"],
        "lines": [120, 300, 80, 150, 220, 60],
        "tests": [4, 12, 3, 6, 9, 2],
    }
)
summary = runs.groupby("language").agg(
    files=("lines", "count"), lines=("lines", "sum"), tests=("tests", "sum")
)
print(summary.sort_values("lines", ascending=False).to_string())
