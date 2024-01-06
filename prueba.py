import pandas as pd
from sqlalchemy import create_engine

user = ""
password = ""
host = ""
db_name = ""
excel_path = ""
excel_sheet = "Transacciones"

result_path = ""

conexion_str = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'
engine = create_engine(conexion_str, echo=True)

df = pd.read_excel(excel_path, sheet_name = excel_sheet)

results = []
df_state = pd.DataFrame()

for _, row in df.iterrows():
    try: 
        date = pd.to_datetime(row['fecha_transaccion'], '%Y-%m-%d')
        amount = row['valor']
        auth_code = str(row['numero_autorizacion']).rjust(6, '0')
        card_number = row['numero_visible']

        query = f"SELECT t.transaccion_id, t.orden_id, t.usuario_id, t.tarjeta_credito_id, " \
                " m.valor_transaccion_moneda_local as local_currency, m.valor_transaccion_usd as usd" \
                " FROM data_tabla_transaccion AS t INNER JOIN data_tabla_transaccion_montos_adicionales AS m " \
                " ON t.transaccion_id = m.transaccion_id " \
                f" WHERE t.codigo_autorizacion = '{auth_code}' " \
                f" and t.fecha_creacion >=  '{date}' - interval 2 day" \
                f" and t.fecha_creacion <=  '{date}' + interval 2 day" 
     
        df_result = pd.read_sql(query, engine)

        if df_result.empty:
            df_state = pd.concat([df_state, pd.DataFrame({'numero_autorizacion': auth_code,
                                                          'fecha_transaccion' : date,
                                                          'numero_visible' : card_number,
                                                          'valor' : amount},
                                                            index=[0])], ignore_index=True)
            continue

        df_result['difference_local_currency'] = (df_result['local_currency'] - amount).abs()
        df_result['difference_usd'] = (df_result['usd'] - amount).abs()
        min_index = (df_result[['difference_local_currency', 'difference_usd']].min(axis=1)).idxmin()

        min_row = df_result.loc[[min_index]]

        min_row = min_row.drop(['difference_local_currency', 'local_currency', 'difference_usd', 'usd'], axis=1)

        results.append(min_row)

    except Exception as err:
        print(f"Error en la consulta en la BD {err}")
    finally: 
       engine.dispose()


df_final = pd.concat(results, axis = 0)

file_name = result_path
with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
    df_final.to_excel(writer, sheet_name='Reconciled Transactions', index=False)
    df_state.to_excel(writer, sheet_name='Unreconciled Transactions', index=False)
