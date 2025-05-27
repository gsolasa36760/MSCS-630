import threading
from collections import Counter
import sys
import string

# Function to process a text segment in a thread
def process_segment(text_segment, index, result_dict):
    # Create a translation table to remove punctuation
    translator = str.maketrans('', '', string.punctuation)
    # Remove punctuation and convert to lowercase
    cleaned_text = text_segment.translate(translator).lower()
    # Split into words
    words = cleaned_text.split()
    # Count word frequency
    result_dict[index] = Counter(words)
    # Print intermediate result for this thread
    print(f"Thread {index + 1} intermediate count: {result_dict[index]}")

# Function to divide the file into N segments
def read_file_segments(filename, num_segments):
    with open(filename, 'r') as f:
        text = f.read()
    total_length = len(text)
    segment_size = total_length // num_segments
    segments = []
    start = 0
    for i in range(num_segments):
        end = start + segment_size
        # Ensure we don't split a word by adjusting the boundary
        if i == num_segments - 1:
            segments.append(text[start:])
        else:
            while end < total_length and text[end].isalnum():
                end += 1
            segments.append(text[start:end])
        start = end
    return segments

# Function to consolidate all thread-level word counts into a final count
def consolidate_counts(segment_counts):
    final_count = Counter()
    for count in segment_counts.values():
        final_count.update(count)
    return final_count

# Main function: handles argument parsing and thread execution
def main():
    if len(sys.argv) != 3:
        print("Usage: python3 word_counter.py <filename> <number_of_segments>")
        return

    filename = sys.argv[1]
    num_segments = int(sys.argv[2])

    # Read and divide the file into segments
    segments = read_file_segments(filename, num_segments)

    threads = []
    segment_counts = {}  # Shared dictionary to store thread results

    # Create and start threads
    for i in range(num_segments):
        thread = threading.Thread(target=process_segment, args=(segments[i], i, segment_counts))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Combine all intermediate counts into a final count
    final_count = consolidate_counts(segment_counts)

    # Print the final result
    print("\nFinal consolidated word frequency:")
    for word, freq in final_count.items():
        print(f"{word}: {freq}")

# Entry point of the script
if __name__ == "__main__":
    sys.argv = ["word_counter.py", "sample.txt", "2"]
    main()
