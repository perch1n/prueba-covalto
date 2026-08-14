-- conteo de filas por etapa
-- verifica que la traza cuadre: raw = stg + errores + duplicados
select 'raw' as etapa, count(*) as filas from `prueba-covalto-data.raw.transacciones`
union all
select 'stg', count(*) from `prueba-covalto-data.stg.transacciones`
union all
select 'errores', count(*) from `prueba-covalto-data.stg.errores`
union all
select 'ctd', count(*) from `prueba-covalto-data.ctd.transacciones`;