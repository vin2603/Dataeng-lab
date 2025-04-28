import json

# Load 3252.json
with open('3252.json') as f:
    data_3252 = json.load(f)

# Load 3256.json
with open('3256.json') as f:
    data_3256 = json.load(f)

# Combine both lists
combined_data = data_3252 + data_3256

# Save into bcsample.json
with open('bcsample.json', 'w') as f:
    json.dump(combined_data, f)

