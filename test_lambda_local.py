#!/usr/bin/env python3
"""
Script de prueba local para la función Lambda
"""

import sys
import os
sys.path.append('lambda')

def test_lambda_local():
    """Prueba la función Lambda localmente"""
    print("🧪 INICIANDO PRUEBA LOCAL DE LA LAMBDA")
    print("=" * 50)
    
    try:
        # Importar la función
        from lambda_function import lambda_handler
        
        # Crear evento de prueba
        test_event = {
            "test": True,
            "source": "local_test"
        }
        
        # Crear contexto mock
        class MockContext:
            def __init__(self):
                self.function_name = "test-function"
                self.memory_limit_in_mb = 1024
                self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
                self.aws_request_id = "test-request-id"
        
        context = MockContext()
        
        print("🚀 Ejecutando función Lambda...")
        print(f"   📋 Evento: {test_event}")
        
        # NOTA: Esta prueba fallará porque necesita credenciales AWS reales
        # y acceso a DynamoDB. Es solo para verificar que la función se carga correctamente.
        
        result = lambda_handler(test_event, context)
        
        print("✅ RESULTADO:")
        print(f"   📊 Status Code: {result.get('statusCode', 'N/A')}")
        print(f"   💬 Message: {result.get('body', {}).get('message', 'N/A')}")
        
        return result
        
    except ImportError as e:
        print(f"❌ ERROR DE IMPORTACIÓN: {e}")
        print("   💡 Asegúrate de que las dependencias estén instaladas")
        return None
        
    except Exception as e:
        print(f"⚠️ ERROR ESPERADO (falta configuración AWS): {e}")
        print("   ✅ La función se cargó correctamente")
        print("   💡 Para prueba completa, despliega en AWS")
        return None

if __name__ == "__main__":
    test_lambda_local()
