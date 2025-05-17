from blackbox import BlackBox
import sys
import random

def main(input_file, stream_size, num_of_asks, output_filename):
    
    random.seed(553)

    bx = BlackBox()
    reservoir = []
    global_count = 0

    with open(output_filename, 'w') as f:
  
        f.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")

        # for each batch
        for _ in range(num_of_asks):
            stream = bx.ask(input_file, stream_size)

            for user in stream:
                global_count += 1
                if global_count <= 100:
                    reservoir.append(user)
                else:
                    # Accept with probability 100 / n
                    if random.random() < 100.0 / global_count:
                        idx = random.randint(0, 99)
                        reservoir[idx] = user

            # After each batch, write seqnum and the five slots
            line_fields = [
                str(global_count),
                reservoir[0],
                reservoir[20],
                reservoir[40],
                reservoir[60],
                reservoir[80],
            ]
            f.write(",".join(line_fields) + "\n")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python task3.py <input_file> <stream_size> <num_of_asks> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_file = sys.argv[4]
    
    main(input_file, stream_size, num_of_asks, output_file)
