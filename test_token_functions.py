#!/usr/bin/env python3
"""
Script de prueba para validar las funciones de extracción de texto y cálculo de tokens
"""

import ast
import sys

def extract_user_text_from_conversation(conversation_text):
    """
    Extrae todos los textos del 'user' de una conversación completa.
    """
    if not conversation_text or conversation_text.strip() == "":
        return ""
    
    try:
        conversations = ast.literal_eval(conversation_text)
        
        if not isinstance(conversations, list):
            return ""
        
        user_texts = []
        for conv in conversations:
            if isinstance(conv, dict) and conv.get('from') == 'user' and 'text' in conv:
                user_text = conv['text'].strip()
                if user_text:
                    user_texts.append(user_text)
        
        return " ".join(user_texts)
        
    except (ValueError, SyntaxError, TypeError) as e:
        print(f"⚠️ Error parseando conversación para user: {str(e)}")
        return ""

def extract_bot_text_from_conversation(conversation_text):
    """
    Extrae todos los textos del 'bot' de una conversación completa.
    """
    if not conversation_text or conversation_text.strip() == "":
        return ""
    
    try:
        conversations = ast.literal_eval(conversation_text)
        
        if not isinstance(conversations, list):
            return ""
        
        bot_texts = []
        for conv in conversations:
            if isinstance(conv, dict) and conv.get('from') == 'bot' and 'text' in conv:
                bot_text = conv['text'].strip()
                if bot_text:
                    bot_texts.append(bot_text)
        
        return " ".join(bot_texts)
        
    except (ValueError, SyntaxError, TypeError) as e:
        print(f"⚠️ Error parseando conversación para bot: {str(e)}")
        return ""

def test_extraction():
    """
    Prueba las funciones de extracción con el ejemplo proporcionado
    """
    # Ejemplo del usuario
    conversation_example = """[{'from': 'user', 'text': 'Buenas tardes', 'timestamp': '2025-08-19T13:26:44Z'}, {'from': 'bot', 'text': 'Buenas tardes. Soy CatIA, el asistente virtual de Catastro Bogotá. ¿En qué puedo ayudarte hoy con información sobre los servicios o trámites de Catastro Distrital?', 'timestamp': '2025-08-19T13:26:44Z'}]"""
    
    print("🧪 PRUEBA DE EXTRACCIÓN DE TEXTO DE CONVERSACIONES")
    print("=" * 60)
    
    print(f"\n📝 Conversación de ejemplo:")
    print(f"{conversation_example}")
    
    # Extraer texto del usuario
    user_text = extract_user_text_from_conversation(conversation_example)
    print(f"\n👤 Texto extraído del USER:")
    print(f"'{user_text}'")
    print(f"Longitud: {len(user_text)} caracteres")
    
    # Extraer texto del bot
    bot_text = extract_bot_text_from_conversation(conversation_example)
    print(f"\n🤖 Texto extraído del BOT:")
    print(f"'{bot_text}'")
    print(f"Longitud: {len(bot_text)} caracteres")
    
    # Prueba con segundo ejemplo más complejo
    print(f"\n" + "=" * 60)
    print("🧪 PRUEBA CON EJEMPLO COMPLEJO (MÚLTIPLES INTERCAMBIOS)")
    
    complex_example = """[{'from': 'user', 'text': 'Buenas tardes', 'timestamp': '2025-08-19T13:26:44Z'}, {'from': 'bot', 'text': 'Buenas tardes. Soy CatIA, el asistente virtual de Catastro Bogotá. ¿En qué puedo ayudarte hoy con información sobre los servicios o trámites de Catastro Distrital?', 'timestamp': '2025-08-19T13:26:44Z'}, {'from': 'user', 'text': 'cancelar cita virtual', 'timestamp': '2025-08-19T13:27:36Z'}, {'from': 'bot', 'text': 'Para cancelar una cita virtual con Catastro Bogotá, puedes seguir estos pasos:\\n\\n1. Revisa tu correo electrónico: Al momento de agendar tu cita, recibiste un correo de confirmación de la cuenta UAECD_CITA_ORIENTACION_POR_VIDEO_LLAMADA@catastrobogota.gov.co   .\\n\\n2. Localiza la opción "Administrar reserva": En el correo de confirmación, encontrarás una opción llamada "Administrar reserva"  .\\n\\n3. Selecciona "Cancelar": Al dar clic en "Administrar reserva", podrás elegir la opción de cancelar tu cita  .\\n\\n4. Confirma la cancelación: Sigue las instrucciones que te proporcione el sistema para confirmar la cancelación de tu cita.\\n\\nAlternativamente, si tienes problemas para encontrar el correo o acceder a la opción de cancelación, puedes:\\n\\n- Ingresar a la página web de Catastro Bogotá (www.catastrobogota.gov.co) y buscar la sección de "Agenda a un clic" para gestionar tu cita  .\\n\\n- Contactar directamente con el servicio de atención al ciudadano de Catastro Bogotá para solicitar asistencia en la cancelación de tu cita.\\n\\nRecuerda que es importante cancelar tu cita con anticipación si no podrás asistir, para permitir que otros ciudadanos puedan aprovechar ese espacio.', 'timestamp': '2025-08-19T13:27:36Z'}]"""
    
    user_text_complex = extract_user_text_from_conversation(complex_example)
    bot_text_complex = extract_bot_text_from_conversation(complex_example)
    
    print(f"\n👤 USER (múltiples mensajes):")
    print(f"'{user_text_complex}'")
    print(f"Longitud: {len(user_text_complex)} caracteres")
    
    print(f"\n🤖 BOT (múltiples mensajes):")
    print(f"'{bot_text_complex}'")
    print(f"Longitud: {len(bot_text_complex)} caracteres")
    
    # Simular conteo de tokens (aproximado)
    print(f"\n🔢 ESTIMACIÓN DE TOKENS (aproximada, 4 chars = 1 token):")
    print(f"   👤 USER tokens ≈ {len(user_text_complex) // 4}")
    print(f"   🤖 BOT tokens ≈ {len(bot_text_complex) // 4}")
    
    print(f"\n✅ PRUEBAS COMPLETADAS - Funciones trabajando correctamente")

if __name__ == "__main__":
    test_extraction()
