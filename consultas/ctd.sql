create or replace table `prueba-covalto-data.ctd.transacciones` as
select
  id_transaccion,
  id_cliente,
  monto,
  tipo,
  fecha_transaccion,
  date(fecha_transaccion) as dia,
  moneda
from `prueba-covalto-data.stg.transacciones`;