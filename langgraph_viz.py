import os
from IPython.display import display, Image
from langgraph.graph import StateGraph

# Import your graph creation function
from langgraph_implementation import create_graph

def visualize_langgraph(output_file="langgraph_viz.png", format="png"):
    """
    Visualize the LangGraph workflow.
    
    Args:
        output_file: Path to save the visualization
        format: Output format (png, svg, pdf)
        
    Returns:
        Path to the generated visualization file
    """
    # Create the graph
    graph = create_graph()
    
    # Get the visualization
    viz = graph.get_graph(xray=True)
    
    # Save the visualization
    if format == "png":
        # For PNG output
        viz_data = viz.draw_mermaid_png()
        with open(output_file, "wb") as f:
            f.write(viz_data)
    elif format == "svg":
        # For SVG output
        viz_data = viz.draw_mermaid_svg()
        with open(output_file, "wb") as f:
            f.write(viz_data)
    elif format == "mermaid":
        # For raw mermaid output
        viz_data = viz.draw_mermaid()
        with open(output_file, "w") as f:
            f.write(viz_data)
    
    print(f"Graph visualization saved to {output_file}")
    return output_file

def display_langgraph():
    """Display the LangGraph workflow in a notebook environment."""
    # Create the graph
    graph = create_graph()
    
    # Get the visualization and display it
    viz = graph.get_graph(xray=True)
    display(Image(viz.draw_mermaid_png()))

# For command-line usage
if __name__ == "__main__":
    output_path = visualize_langgraph()
    print(f"Visualization saved to: {output_path}")