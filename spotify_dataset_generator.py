import time
import pickle
import os
from scapy.all import sniff
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

# Load .env file from current directory
load_dotenv()

# Configuration
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")
DATASET_FILE = "spotify_traffic_dataset.pkl"
CAPTURE_DURATION = 60  # seconds

# List of song URIs to capture (replace with your test songs)
SONG_URIS = [
    "spotify:track:5SudOD9R1Of6CsJVWZy6CQ",
    "spotify:track:4oVO4fGNRRvEn0CRuFO4qv",
    "spotify:track:6hpuesKPNa3WhV48O7Fa47"
]


class SpotifyDatasetGenerator:
    def __init__(self):
        self.driver = None
        self.spotify_client = None
        self.captured_data = []
                
    def setup_spotify_client(self):
        """Initialize Spotipy client"""
        print("Setting up Spotify client...")
        scope = "user-modify-playback-state user-read-playback-state"
        self.spotify_client = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=SPOTIFY_REDIRECT_URI,
            scope=scope
        ))
        print("   ✓ Spotify client authenticated")
    
    def packet_callback(self, packet):
        """Callback function for packet capture"""
        arrival_time = packet.time
        payload_size = len(packet)
        self.current_capture.append((arrival_time, payload_size))
    
    def capture_song_traffic(self, song_uri, song_index):
        """Capture network traffic for a specific song"""
        print(f"[3-4] Capturing traffic for song {song_index + 1}/3...")
        
        # Start playback using Spotipy
        try:
            self.spotify_client.start_playback(uris=[song_uri])
            print(f"   ✓ Started playback: {song_uri}")
            time.sleep(2)  # Wait for playback to stabilize
        except Exception as e:
            print(f"   ⚠ Error starting playback: {e}")
            print("   Attempting to continue with current playback...")
        
        # Initialize capture list for this song
        self.current_capture = []
        
        # Start packet sniffing
        print(f"   Sniffing packets for {CAPTURE_DURATION} seconds...")
        start_time = time.time()
        try:
            sniff(
                iface="ens33",
                prn=self.packet_callback,
                timeout=CAPTURE_DURATION,
                store=False
            )
        except PermissionError:
            print("   ⚠ ERROR: Permission denied. Please run script with sudo/admin privileges")
            raise
        
        elapsed = time.time() - start_time
        print(f"   ✓ Captured {len(self.current_capture)} packets in {elapsed:.1f}s")
        
        # Store the capture
        return self.current_capture.copy()
    
    def save_dataset(self, new_data):
        """Save or append data to pickle file"""
        print("[6] Saving dataset...")
        
        # Load existing data if file exists
        if os.path.exists(DATASET_FILE):
            with open(DATASET_FILE, 'rb') as f:
                existing_data = pickle.load(f)
            print(f"   Loaded existing dataset with {len(existing_data)} samples")
            existing_data.extend(new_data)
            data_to_save = existing_data
        else:
            print("   Creating new dataset file")
            data_to_save = new_data
        
        # Save updated dataset
        with open(DATASET_FILE, 'wb') as f:
            pickle.dump(data_to_save, f)
        
        print(f"   ✓ Saved dataset with {len(data_to_save)} total samples")
        print(f"   ✓ File: {DATASET_FILE}")
    
    def generate_dataset(self):
        """Main method to generate the dataset"""
        try:
            # Setup
            self.setup_spotify_client()
            
            # Capture data for each song
            captured_songs = []
            for i, song_uri in enumerate(SONG_URIS):
                song_data = self.capture_song_traffic(song_uri, i)
                captured_songs.append(song_data)
                
                # Small pause between songs
                if i < len(SONG_URIS) - 1:
                    print("   Pausing before next song...")
                    time.sleep(5)
            
            # Save the dataset
            self.save_dataset(captured_songs)
            
            print("\n" + "="*50)
            print("Dataset generation complete!")
            print(f"Total songs captured: {len(captured_songs)}")
            for i, song_data in enumerate(captured_songs):
                print(f"  Song {i+1}: {len(song_data)} packets")
            print("="*50)
            
        except KeyboardInterrupt:
            print("\n\nCapture interrupted by user")
        except Exception as e:
            print(f"\n\nError during capture: {e}")
            raise


if __name__ == "__main__":
    print("=" * 50)
    print("Spotify Traffic Dataset Generator")
    print("=" * 50)
    print("\nIMPORTANT NOTES:")
    print("1. Run this script with sudo/administrator privileges")
    print("2. Update SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in the env file")
    print("3. Update SONG_URIS with your test songs")
    print("4. Install required packages:")
    print("   pip install -r requirements.txt")
    print("=" * 50 + "\n")
    
    input("Press Enter to start data collection...")
    
    generator = SpotifyDatasetGenerator()
    generator.generate_dataset()