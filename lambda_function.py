import boto3
import csv
import io
from datetime import datetime

# Conexiones a los servicios
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('VentasDiariasKpis')

def lambda_handler(event, context):
    # 1. Obtener detalles del archivo subido
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # 2. Leer el CSV desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))
    
    total_ventas = 0.0
    tickets = 0
    productos = {}
    tienda_id = "DESCONOCIDA"
    fecha = str(datetime.now().date())

    # 3. Procesar filas
    for row in reader:
        # Asegúrate de que los nombres de columnas coincidan con tu CSV
        tienda_id = row.get('tienda_id', 'TIENDA_01')
        fecha = row.get('fecha', fecha)
        precio = float(row.get('precio', 0))
        
        total_ventas += precio
        tickets += 1
        
        prod = row.get('producto', 'Varios')
        productos[prod] = productos.get(prod, 0) + 1

    # 4. Calcular el producto más vendido
    top_producto = max(productos, key=productos.get) if productos else "N/A"

    # 5. Guardar KPIs en DynamoDB
    table.put_item(
        Item={
            'TiendaID': tienda_id,
            'Fecha': fecha,
            'VentasTotales': str(total_ventas),
            'NumTickets': tickets,
            'TopProducto': top_producto,
            'UltimaActualizacion': str(datetime.now())
        }
    )
    
    return f"Procesado exitosamente: {tienda_id}"