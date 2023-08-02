import pytest

from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.3.0"

    def create_test_data(spark, config):
        """Create test data.

        This function creates both both pre- and post- transformation data
        saved as Parquet files in tests/test_data. This will be used for
        unit tests as well as to load as part of the example ETL job.
        :return: None
        """
        # create example data from scratch
        local_records = [
            Row(id=1, first_name='Dan', second_name='Germain', floor=1),
            Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
            Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
            Row(id=4, first_name='Ken', second_name='Lai', floor=2),
            Row(id=5, first_name='Stu', second_name='White', floor=3),
            Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
            Row(id=7, first_name='Phil', second_name='Bird', floor=4),
            Row(id=8, first_name='Kim', second_name='Suter', floor=4)
        ]

        df = spark.createDataFrame(local_records)

        # write to Parquet file format
        (df
         .coalesce(1)
         .write
         .parquet('tests/test_data/employees', mode='overwrite'))



