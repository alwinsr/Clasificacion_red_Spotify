#!/usr/bin/env python3
"""
Script para capturar tráfico de Spotify y crear dataset para QoE/QoS
Basado en el paper de Schwind et al. (2018)

Añadido:
- Columna 'PC': 1 si la captura es desde PC/VM, 0 si es desde móvil.
"""

import pyshark
import pandas as pd
import time
from datetime import datetime
import os

class SpotifyTrafficCapture:
    def __init__(self, interface='eth0', output_dir='spotify_dataset', device_type='PC'):
        self.interface = interface
        self.output_dir = output_dir
        self.capture_data = []
        self.device_type = device_type  # 'PC' o 'Mobile'
        
        # Calidades de Spotify según tu imagen
        self.quality_levels = {
            'Baja': {'bitrate': 24, 'data_hour': 0.01},
            'Normal': {'bitrate': 96, 'data_hour': 0.04},
            'Alta': {'bitrate': 160, 'data_hour': 0.07},
            'Muy alta': {'bitrate': 320, 'data_hour': 0.14},
            'Sin pérdida': {'bitrate': None, 'format': 'FLAC'}  # Hasta 1GB/hora
        }
        
        # Crear directorio de salida
        os.makedirs(output_dir, exist_ok=True)
    
    # --- Filtros ---
    def get_spotify_capture_filter(self):
        """
        Filtro BPF para capturar solo tráfico HTTPS (Spotify va sobre TLS 443).
        """
        return 'tcp port 443'
    
    def get_spotify_display_filter(self):
        """
        Filtro de display: nos quedamos con todo el tráfico TLS (handshake + datos).
        En esta VM casi todo el TLS:443 será Spotify.
        """
        return 'tls'
    
    def start_capture(self, duration_seconds=None, quality_setting='Normal'):
        """
        Inicia la captura de tráfico
        
        Args:
            duration_seconds: Duración de la captura (None = hasta interrupción manual)
            quality_setting: Calidad configurada en Spotify
        """
        print(f"[{datetime.now()}] Iniciando captura...")
        print(f"Calidad configurada: {quality_setting}")
        print(f"Bitrate esperado: {self.quality_levels[quality_setting]['bitrate']} kbps")
        print(f"Interface: {self.interface}")
        print(f"Dispositivo: {self.device_type} (PC=1, Mobile=0 en la columna 'PC')")
        print("\n⚠️  IMPORTANTE: Inicia la reproducción en Spotify AHORA\n")
        
        bpf_filter = self.get_spotify_capture_filter()
        display_filter = self.get_spotify_display_filter()
        
        try:
            capture = pyshark.LiveCapture(
                interface=self.interface,
                bpf_filter=bpf_filter,
                display_filter=display_filter
            )
            
            start_time = time.time()
            packet_count = 0
            
            for packet in capture.sniff_continuously():
                try:
                    packet_count += 1
                    
                    # Extraer información del paquete
                    packet_info = self.extract_packet_info(packet, quality_setting)
                    
                    if packet_info:
                        self.capture_data.append(packet_info)
                    
                    # Mostrar progreso cada 100 paquetes
                    if packet_count % 100 == 0:
                        elapsed = time.time() - start_time
                        print(f"Paquetes capturados: {packet_count} | Tiempo: {elapsed:.1f}s")
                    
                    # Detener si se alcanza la duración
                    if duration_seconds and (time.time() - start_time) >= duration_seconds:
                        break
                        
                except AttributeError:
                    # Paquete sin la información necesaria
                    continue
                    
        except KeyboardInterrupt:
            print("\n[!] Captura detenida por el usuario")
        
        print(f"\n✓ Captura finalizada: {len(self.capture_data)} paquetes procesados")
        return self.capture_data
    
    def extract_packet_info(self, packet, quality_setting):
        """
        Extrae información relevante del paquete para QoE/QoS
        """
        try:
            packet_data = {
                'timestamp': float(packet.sniff_timestamp),
                'quality_setting': quality_setting,
                'expected_bitrate': self.quality_levels[quality_setting]['bitrate'],
                'protocol': packet.highest_layer,
                'length': int(packet.length),
                # NUEVA COLUMNA: PC (1 = PC/VM, 0 = móvil)
                'PC': 1 if self.device_type == 'PC' else 0,
            }
            
            # Información TCP
            if hasattr(packet, 'tcp'):
                packet_data.update({
                    'src_port': int(packet.tcp.srcport),
                    'dst_port': int(packet.tcp.dstport),
                    'tcp_flags': packet.tcp.flags,
                    'seq_num': int(packet.tcp.seq),
                    'ack_num': int(packet.tcp.ack) if hasattr(packet.tcp, 'ack') else None,
                })
            
            # Información IP
            if hasattr(packet, 'ip'):
                packet_data.update({
                    'src_ip': packet.ip.src,
                    'dst_ip': packet.ip.dst,
                    'ttl': int(packet.ip.ttl),
                })
            
            # Información TLS (Spotify usa HTTPS)
            if hasattr(packet, 'tls'):
                packet_data['is_spotify_tls'] = True
                if hasattr(packet.tls, 'record_length'):
                    packet_data['tls_record_length'] = int(packet.tls.record_length)
            
            return packet_data
            
        except Exception:
            return None
    
    def save_dataset(self, filename=None):
        """
        Guarda los datos capturados en formato CSV
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'spotify_traffic_{timestamp}.csv'
        
        filepath = os.path.join(self.output_dir, filename)
        
        df = pd.DataFrame(self.capture_data)
        
        if not df.empty:
            # Calcular métricas agregadas
            df = self.calculate_metrics(df)
            df.to_csv(filepath, index=False)
            print(f"\n✓ Dataset guardado: {filepath}")
            print(f"  Total registros: {len(df)}")
            print(f"\nPrimeras filas del dataset:")
            print(df.head())
            
            # Guardar resumen estadístico
            self.save_summary(df, filepath.replace('.csv', '_summary.txt'))
        else:
            print("⚠️  No hay datos para guardar")
        
        return filepath
    
    def calculate_metrics(self, df):
        """
        Calcula métricas de QoS relevantes según el paper
        """
        # Ordenar por timestamp
        df = df.sort_values('timestamp')
        
        # Calcular intervalos entre paquetes (IAT - Inter Arrival Time)
        df['iat'] = df['timestamp'].diff()
        
        # Throughput por ventana de tiempo (cada segundo)
        df['second'] = df['timestamp'].astype(int)
        throughput = df.groupby('second')['length'].sum() * 8 / 1000  # kbps
        df = df.merge(throughput.rename('throughput_kbps'), 
                      left_on='second', right_index=True, how='left')
        
        return df
    
    def save_summary(self, df, filepath):
        """
        Guarda un resumen estadístico de la captura
        """
        with open(filepath, 'w') as f:
            f.write("=== RESUMEN DE CAPTURA SPOTIFY ===\n\n")
            f.write(f"Calidad configurada: {df['quality_setting'].iloc[0]}\n")
            f.write(f"Bitrate esperado: {df['expected_bitrate'].iloc[0]} kbps\n\n")
            
            f.write("--- Estadísticas de tráfico ---\n")
            f.write(f"Total paquetes: {len(df)}\n")
            f.write(f"Duración captura: {df['timestamp'].max() - df['timestamp'].min():.2f} segundos\n")
            f.write(f"Tamaño total: {df['length'].sum() / 1024 / 1024:.2f} MB\n")
            
            if 'throughput_kbps' in df.columns:
                f.write(f"\nThroughput promedio: {df['throughput_kbps'].mean():.2f} kbps\n")
                f.write(f"Throughput máximo: {df['throughput_kbps'].max():.2f} kbps\n")
                f.write(f"Throughput mínimo: {df['throughput_kbps'].min():.2f} kbps\n")
            
            if 'iat' in df.columns:
                f.write(f"\nIAT promedio: {df['iat'].mean()*1000:.2f} ms\n")
                f.write(f"IAT máximo: {df['iat'].max()*1000:.2f} ms\n")
        
        print(f"✓ Resumen guardado: {filepath}")


def main():
    """
    Ejemplo de uso del script
    """
    print("=== CAPTURA DE TRÁFICO SPOTIFY PARA QoE/QoS ===\n")
    
    # Configuración
    interface = input("Interface de red (ej: eth0, wlan0, ens33): ").strip()
    
    print("\nDispositivo de captura:")
    print("1. PC / VM (escritorio)")
    print("2. Móvil")
    device_choice = input("Selecciona el dispositivo (1-2): ").strip()
    device_type = 'PC' if device_choice == '1' else 'Mobile'
    
    print("\nCalidades disponibles:")
    print("1. Baja (24 kbps)")
    print("2. Normal (96 kbps)")
    print("3. Alta (160 kbps)")
    print("4. Muy alta (320 kbps)")
    print("5. Sin pérdida (FLAC)")
    
    quality_map = {
        '1': 'Baja',
        '2': 'Normal',
        '3': 'Alta',
        '4': 'Muy alta',
        '5': 'Sin pérdida'
    }
    
    quality_choice = input("\nSelecciona la calidad configurada en Spotify (1-5): ").strip()
    quality = quality_map.get(quality_choice, 'Normal')
    
    duration = input("\nDuración de captura en segundos (Enter para captura continua): ").strip()
    duration = int(duration) if duration else None
    
    # Crear capturador
    capturer = SpotifyTrafficCapture(interface=interface, device_type=device_type)
    
    # Iniciar captura
    print("\n" + "="*60)
    capturer.start_capture(duration_seconds=duration, quality_setting=quality)
    
    # Guardar dataset
    capturer.save_dataset()
    
    print("\n✓ Proceso completado")
    print("\nPróximos pasos:")
    print("1. Repetir para todas las calidades")
    print("2. Capturar múltiples sesiones por calidad")
    print("3. Más adelante: añadir capturas desde móvil (PC=0)")
    print("4. Analizar y limpiar el dataset y entrenar ML")


if __name__ == "__main__":
    main()
