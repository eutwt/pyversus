# --8<-- [start:setup]
import duckdb

from versus import compare, examples

con = duckdb.connect()
example_a = examples.example_cars_a(con)
example_b = examples.example_cars_b(con)
# --8<-- [end:setup]

# --8<-- [start:comparison]
comparison = compare(example_a, example_b, by=["car"], connection=con)
print(comparison)
# --8<-- [end:comparison]

# --8<-- [start:value-diffs]
print(comparison.value_diffs("disp"))
# --8<-- [end:value-diffs]

# --8<-- [start:value-diffs-stacked]
print(comparison.value_diffs_stacked(["mpg", "disp"]))
# --8<-- [end:value-diffs-stacked]

# --8<-- [start:weave-diffs-wide-disp]
print(comparison.weave_diffs_wide(["disp"]))
# --8<-- [end:weave-diffs-wide-disp]

# --8<-- [start:weave-diffs-wide-mpg-disp]
print(comparison.weave_diffs_wide(["mpg", "disp"]))
# --8<-- [end:weave-diffs-wide-mpg-disp]

# --8<-- [start:weave-diffs-long-disp]
print(comparison.weave_diffs_long(["disp"]))
# --8<-- [end:weave-diffs-long-disp]

# --8<-- [start:slice-diffs]
print(comparison.slice_diffs("a", ["mpg"]))
# --8<-- [end:slice-diffs]

# --8<-- [start:slice-unmatched]
print(comparison.slice_unmatched("a"))
# --8<-- [end:slice-unmatched]

# --8<-- [start:slice-unmatched-both]
print(comparison.slice_unmatched_both())
# --8<-- [end:slice-unmatched-both]

# --8<-- [start:summary]
print(comparison.summary())
# --8<-- [end:summary]
