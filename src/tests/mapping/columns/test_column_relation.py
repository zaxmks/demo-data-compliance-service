from src.mapping.columns.column_relation import ColumnRelation


def test_init():
    cr = ColumnRelation("target", "source_column", "target_column", 0.5)
    assert cr.target_data_source == "target"
    assert cr.source_column_name == "source_column"
    assert cr.target_column_name == "target_column"
    assert cr.confidence == 0.5


def test_str():
    cr = ColumnRelation("target", "source_column", "target_column", 0.5)
    assert str(cr) == "source_column -> target: target_column (0.50 conf)"
