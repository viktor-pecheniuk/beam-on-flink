from datetime import datetime

import apache_beam as beam


class ExtractFields(beam.DoFn):

    def process(self, element):
        fields = element.get("row")
        if fields.get("@Tags") is not None:
            duration = datetime.fromisoformat(
                fields.get("@LastActivityDate")) - datetime.fromisoformat(fields.get("@CreationDate"))
            # filter rows whether updated_date - created_date is more than 2 years (365 * 2 => 730)
            if duration.days >= 730:
                out = self.reformat_record(fields)
                yield out

    @staticmethod
    def reformat_record(field):
        return beam.Row(
            post_id=field.get("@Id"),
            title=field.get("@Title"),
            tags=field.get("@Tags"),
            updated_date=field.get("@LastActivityDate"),
            created_date=field.get("@CreationDate")
        )
