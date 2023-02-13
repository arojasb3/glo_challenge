#main.py
from flask import Flask, jsonify, request

app = Flask(__name__)
songs = [
    {
        "title": "Rockstar",
        "artist": "Dababy",
        "genre": "rap",
    },
    {
        "title": "Say So",
        "artist": "Doja Cat",
        "genre": "Hiphop",
    },
    {
        "title": "Panini",
        "artist": "Lil Nas X",
        "genre": "Hiphop"
    }
]

@app.route('/', methods=['POST', 'GET'])
def songs():
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"msg": "Missing JSON in request"}), 400  

        add_songs(request.get_json())
        return 'Song Added'

@app.route('/songs')
def home():
    return jsonify(songs)

@app.route('/songs', methods=['POST'])
def add_songs():
    song = request.get_json()
    songs.append(song)
    return jsonify(songs)


if __name__ == '__main__':
  app.run(debug=True)
