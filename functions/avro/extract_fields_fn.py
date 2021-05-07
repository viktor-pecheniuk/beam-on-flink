import apache_beam as beam


class ExtractFieldsFn(beam.DoFn):

    def process(self, element):
        file_row = element
        if file_row.get("tags") is not None:
            duration = file_row.get("last_activity_date") - file_row.get("creation_date")
            # filter rows whether updated_date - created_date is more than 2 years (365 * 2 => 730)
            if duration.days >= 730:
                out = self.reformat_record(file_row)
                yield out

    @staticmethod
    def reformat_record(field):
        post_id = str(field.get("id"))
        return beam.Row(
            post_id=post_id,
            title=field.get("title"),
            tags=field.get("tags"),
        )
