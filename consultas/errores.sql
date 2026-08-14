-- errores agrupados por motivo
select motivo_error, count(*) as filas
from `prueba-covalto-data.stg.errores`
group by motivo_error;

-- detalle de los registros rechazados
select * from `prueba-covalto-data.stg.errores`
order by motivo_error;