
# TODO: two columns tables are index tables?
# TODO: why page numbers-1? 0 excluded as special?
# TODO: traverse is wrong somehow
# TODO:   chinook.db => [sqlite3_table("Track")] | length => 3496, should be 3503 rows

def sqlite3_traverse($root; $page):
  def _t:
    ( . # debug({TRAVESE: .})
    | if .type == "table_interior" or .type == "index_interior" then
        ( $root.pages[.cells[].left_child-1, .right_pointer-1]
        | _t
        )
      elif .type == "table_leaf" or .type == "index_leaf" then
        ( .cells[]
        )
      end
    );
  ( $page
  | _t
  );

def sqlite3_table($name):
  ( . as $root
  | ( first(
        ( sqlite3_traverse($root; $root.pages[0])
        | select(.payload.contents | .[0] == "table" and .[2] == $name)
        )
      )
    ) as $table_start_cell
  | ( first(
        ( sqlite3_traverse($root; $root.pages[0])
        | select(.payload.contents| .[0] == "index" and .[2] == $name)
        )
      )
    ) as $index_start_cell
  | sqlite3_traverse($root; $root.pages[$index_start_cell.payload.contents[3]-1]) as $index_row
  | sqlite3_traverse($root; $root.pages[$table_start_cell.payload.contents[3]-1])
  | first(select(.rowid == $index_row.payload.contents[1]))
  | .payload.contents
  );
