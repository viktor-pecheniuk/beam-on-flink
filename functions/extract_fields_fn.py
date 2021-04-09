import re
import apache_beam as beam

from datetime import datetime


class ExtractFields(beam.DoFn):

    def process(self, element):
        file_rows = element.get('posts').get("row")
        for row in file_rows:
            if not row.get("@Tags"):
                continue
            duration = datetime.fromisoformat(
                row.get("@LastActivityDate")) - datetime.fromisoformat(row.get("@CreationDate"))
            # filter rows whether updated_date - created_date is more than 2 years (365 * 2 => 730)
            if duration.days <= 730:
                continue
            record = self.reformat_row(row)
            yield record

    @staticmethod
    def reformat_row(row):
        return beam.Row(
            post_id=row.get("@Id"),
            title=row.get("@Title"),
            tags=row.get("@Tags"),
            updated_date=row.get("@LastActivityDate"),
            created_date=row.get("@CreationDate")
        )
