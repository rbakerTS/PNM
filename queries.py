import pandas as pd

from basic_db_manager import BasicDatabaseManager

if __name__ == '__main__':
    engine_str = 'mssql+pyodbc://TS-TYLER/PNM_2021_Pole_Attachment_Audit?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0'
    d = BasicDatabaseManager(connection_str=engine_str)

    table_name = "tblKatapultPoleQAQCData_v2"
    column_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    columns = d.query(column_query, return_rows=True)
    columns = [column[0] for column in columns]

    # total poles audited
    total_poles_audited_query = f"SELECT COUNT(*) FROM {table_name}"
    total_poles_audited_rows = d.query(total_poles_audited_query, return_rows=True)
    total_poles_audited = total_poles_audited_rows[0][0]

    # total poles with attachments
    total_poles_with_attachers_query = f"SELECT COUNT(*) FROM {table_name} WHERE Attacher_1 is not null  "
    total_poles_with_attachers_rows = d.query(total_poles_with_attachers_query, return_rows=True)
    total_poles_with_attachers = total_poles_with_attachers_rows[0][0]

    # total foreign poles (pole owner does not equal PNM)
    total_foreign_poles_query = f"SELECT COUNT(*) FROM {table_name} WHERE OWNER != 'PNM'"
    total_foreign_poles_rows = d.query(total_foreign_poles_query, return_rows=True)
    total_foreign_poles = total_foreign_poles_rows[0][0]

    # total poles without attachments (delta between poles audited and poles with attachments)
    total_poles_without_attachers_query = f"SELECT COUNT(*) FROM {table_name} WHERE Attacher_1 is null  "
    total_poles_without_attachers_rows = d.query(total_poles_without_attachers_query, return_rows=True)
    total_poles_without_attachers = total_poles_without_attachers_rows[0][0]

    data_dict = {
        'total poles audited': total_poles_audited,
        'total poles with attachments':total_poles_with_attachers,
        'total foreign poles (pole owner does not equal PNM)':total_foreign_poles,
        'total poles without attachments':total_poles_without_attachers

    }
    df = pd.DataFrame([data_dict])
    df.to_csv('analysis_08_04_2022.csv',index=False)

    quit()