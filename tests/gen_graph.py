def generate_networked_volume_graphs(csv_file: str):
    """Generate graphs from the networked volume test data"""
    import pandas as pd
    import matplotlib.pyplot as plt
    
    # Read the CSV data
    df = pd.read_csv(csv_file)
    
    # Create figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
    
    # Plot 1: File Count over Time (for create operations)
    create_data = df[df['operation_type'] == 'create']
    for peer_count in range(2, 7, 2):
        peer_data = create_data[create_data['peer_count'] == peer_count]
        ax1.plot(peer_data['timestamp'], peer_data['cumulative_files'], 
                label=f'{peer_count} peer{"s" if peer_count > 1 else ""}')
    ax1.set_title('Cumulative File Count Over Time (Create Empty Files)')
    ax1.set_xlabel('Time (Seconds)')
    ax1.set_ylabel('Number of Files')
    ax1.legend()
    ax1.grid(True)
    
    # Plot 2: Data Written over Time (for create_with_data operations)
    create_with_data = df[df['operation_type'] == 'create_with_data']
    for peer_count in range(2, 7, 2):
        peer_data = create_with_data[create_with_data['peer_count'] == peer_count]
        ax2.plot(peer_data['timestamp'], peer_data['cumulative_data'], 
                label=f'{peer_count} peer{"s" if peer_count > 1 else ""}')
    ax2.set_title('Cumulative Data Written (Create with 4B Data)')
    ax2.set_xlabel('Time (Seconds)')
    ax2.set_ylabel('Data Written (bytes)')
    ax2.legend()
    ax2.grid(True)
    
    # Plot 3: Data Written over Time (for append operations)
    append_data = df[df['operation_type'] == 'append_data']
    for peer_count in range(2, 7, 2):
        peer_data = append_data[append_data['peer_count'] == peer_count]
        ax3.plot(peer_data['timestamp'], peer_data['cumulative_data'], 
                label=f'{peer_count} peer{"s" if peer_count > 1 else ""}')
    ax3.set_title('Cumulative Data Written (Append 4B Data)')
    ax3.set_xlabel('Time (Seconds)')
    ax3.set_ylabel('Data Written (bytes)')
    ax3.legend()
    ax3.grid(True)
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig('networked_volume_graphs.png')
    plt.close()
    
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python gen_graph.py <csv_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    generate_networked_volume_graphs(csv_file)
    print(f"Graphs generated and saved as 'networked_volume_graphs.png'")