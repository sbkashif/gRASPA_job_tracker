import sys
import re

def extract_averages(filename):
    with open(filename, 'r') as file:
        content = file.read()
        
        # Extract unit cell information
        unit_cells_pattern = r'Total Unit Cells (\d+)'
        unit_cells_match = re.search(unit_cells_pattern, content)
        unit_cells = int(unit_cells_match.group(1)) if unit_cells_match else 1
        
        # Extract pressure and temperature
        pressure_pattern = r'Pressure:\s+([\d\.]+)'
        temp_pattern = r'Box Temperature:\s+([\d\.]+)'
        pressure_match = re.search(pressure_pattern, content)
        temp_match = re.search(temp_pattern, content)
        
        pressure = float(pressure_match.group(1)) if pressure_match else None
        temperature = float(temp_match.group(1)) if temp_match else None
        
        # Extract framework mass
        framework_mass_pattern = r'Framework total mass:\s+([\d\.]+)'
        framework_mass_match = re.search(framework_mass_pattern, content)
        framework_mass = float(framework_mass_match.group(1)) if framework_mass_match else None
        
        # Extract average volume
        volume_pattern = r'=====================BLOCK AVERAGES \(VOLUME Å\^3\)================.*?Overall: Average: ([\d\.]+)'
        volume_match = re.search(volume_pattern, content, re.DOTALL)
        average_volume = float(volume_match.group(1)) if volume_match else None
        
        # Initialize results dictionary
        results = {
            'unit_cells': unit_cells,
            'pressure': pressure,
            'temperature': temperature,
            'framework_mass': framework_mass,
            'average_volume': average_volume
        }
        
        # Define all the sections we want to extract
        section_patterns = [
            ('heat_of_adsorption_kJ_mol', r'BLOCK AVERAGES \(HEAT OF ADSORPTION: kJ/mol\)'),
            ('loading_num_molecules', r'BLOCK AVERAGES \(LOADING: # MOLECULES\)'),
            ('loading_mg_g', r'BLOCK AVERAGES \(LOADING: mg/g\)'),
            ('loading_mol_kg', r'BLOCK AVERAGES \(LOADING: mol/kg\)'),
            ('loading_g_L', r'BLOCK AVERAGES \(LOADING: g/L\)')
        ]
        
        # Process each section
        for section_key, section_header in section_patterns:
            # Create a pattern to extract the CO2 and N2 component data
            pattern = f"{section_header}.*?COMPONENT \\[1\\] \\(CO2\\)(.*?)COMPONENT \\[2\\] \\(N2\\)(.*?)----------------------------------------------------------"
            match = re.search(pattern, content, re.DOTALL)
            
            if match:
                co2_section = match.group(1)
                n2_section = match.group(2)
                
                # Extract the overall average and error bar
                co2_match = re.search(r'Overall: Average: ([-\d\.]+), ErrorBar: ([-\d\.]+)', co2_section)
                n2_match = re.search(r'Overall: Average: ([-\d\.]+), ErrorBar: ([-\d\.]+)', n2_section)
                
                if co2_match and n2_match:
                    results[section_key] = {
                        'CO2': {
                            'average': float(co2_match.group(1)),
                            'error': float(co2_match.group(2))
                        },
                        'N2': {
                            'average': float(n2_match.group(1)),
                            'error': float(n2_match.group(2))
                        }
                    }
        
        # Special handling for mole fractions - they're in a different format
        mol_fractions_pattern = r'================= MOL FRACTIONS =================\s+' + \
                                r'Component \[1\] \(CO2\), Mol Fraction: ([\d\.]+)\s+' + \
                                r'Component \[2\] \(N2\), Mol Fraction: ([\d\.]+)'
        
        mf_match = re.search(mol_fractions_pattern, content)
        if mf_match:
            co2_mf = float(mf_match.group(1))
            n2_mf = float(mf_match.group(2))
            
            results['mole_fraction'] = {
                'CO2': {
                    'average': co2_mf,
                    'error': 0.0  # No error reported for mole fractions
                },
                'N2': {
                    'average': n2_mf,
                    'error': 0.0  # No error reported for mole fractions
                }
            }
        
        # Check if we found at least the molecules loading data (for backward compatibility)
        if 'loading_num_molecules' in results:
            # For backward compatibility, copy the loading_num_molecules data to the top level
            results['CO2'] = results['loading_num_molecules']['CO2']
            results['N2'] = results['loading_num_molecules']['N2']
            return results
        
        return None

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py filename")
        sys.exit(1)

    filename = sys.argv[1]
    result = extract_averages(filename)

    if result:
        print(f"File: {filename}")
        print(f"Pressure: {result['pressure']:.5f} Pa, Temperature: {result['temperature']:.5f} K")
        print(f"Total Unit Cells: {result['unit_cells']}")
        print(f"Framework Mass: {result['framework_mass']:.5f} g")
        print(f"Average Volume: {result['average_volume']:.5f} Å^3")
        
        # Print information for each section
        print("\n=== MOLECULE LOADING ===")
        print(f"CO2: {result['CO2']['average']:.5f} ± {result['CO2']['error']:.5f} molecules")
        print(f"N2: {result['N2']['average']:.5f} ± {result['N2']['error']:.5f} molecules")
        print(f"CO2 per Unit Cell: {result['CO2']['average']/result['unit_cells']:.5f}")
        print(f"N2 per Unit Cell: {result['N2']['average']/result['unit_cells']:.5f}")
        print(f"CO2/N2 Selectivity: {result['CO2']['average']/result['N2']['average']:.5f}")
        
        # Heat of adsorption
        if 'heat_of_adsorption_kj_mol' in result:
            print("\n=== HEAT OF ADSORPTION ===")
            print(f"CO2: {result['heat_of_adsorption_kj_mol']['CO2']['average']:.5f} ± {result['heat_of_adsorption_kj_mol']['CO2']['error']:.5f} kJ/mol")
            print(f"N2: {result['heat_of_adsorption_kj_mol']['N2']['average']:.5f} ± {result['heat_of_adsorption_kj_mol']['N2']['error']:.5f} kJ/mol")
        
        # Loading in mg/g
        if 'loading_mg_g' in result:
            print("\n=== LOADING (mg/g) ===")
            print(f"CO2: {result['loading_mg_g']['CO2']['average']:.5f} ± {result['loading_mg_g']['CO2']['error']:.5f} mg/g")
            print(f"N2: {result['loading_mg_g']['N2']['average']:.5f} ± {result['loading_mg_g']['N2']['error']:.5f} mg/g")
        
        # Loading in mol/kg
        if 'loading_mol_kg' in result:
            print("\n=== LOADING (mol/kg) ===")
            print(f"CO2: {result['loading_mol_kg']['CO2']['average']:.5f} ± {result['loading_mol_kg']['CO2']['error']:.5f} mol/kg")
            print(f"N2: {result['loading_mol_kg']['N2']['average']:.5f} ± {result['loading_mol_kg']['N2']['error']:.5f} mol/kg")
        
        # Loading in g/L
        if 'loading_g_L' in result:
            print("\n=== LOADING (g/L) ===")
            print(f"CO2: {result['loading_g_L']['CO2']['average']:.5f} ± {result['loading_g_L']['CO2']['error']:.5f} g/L")
            print(f"N2: {result['loading_g_L']['N2']['average']:.5f} ± {result['loading_g_L']['N2']['error']:.5f} g/L")
    else:
        print(f"Could not extract averages from {filename}")