# genera un csv de transacciones para probar el pipeline
# le mete errores a proposito para probar la validacion

import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)
n = 5000
fecha_base = datetime(2026, 8, 13)

datos = []
for i in range(n):
    datos.append({
        "id_transaccion": f"TRX{i:06d}",
        "id_cliente": f"CLI{random.randint(1, 500):04d}",
        "monto": round(random.uniform(10, 15000), 2),
        "tipo": random.choice(["compra", "transferencia", "pago", "retiro"]),
        "fecha_transaccion": (fecha_base - timedelta(minutes=random.randint(0, 1440))).isoformat(),
        "moneda": random.choice(["MXN", "USD"]),
    })

df = pd.DataFrame(datos)

# errores a proposito
df.loc[10:15, "monto"] = -50                    # montos negativos
df.loc[20:22, "id_cliente"] = ""                 # clientes vacios
df.loc[30:32, "id_transaccion"] = "TRX000001"    # duplicados

df.to_csv("datos/transacciones.csv", index=False)
print(f"listo: {len(df)} filas")