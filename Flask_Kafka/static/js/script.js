function login() {
  console.log('login function called');
  const email = document.querySelector('#email').value;
  const password = document.querySelector('#password').value;
  
  if (email === '123@gmail.com' && password === '12345') {
      // Redirect the user to the result page
      window.location.href = '/result';
  } else {
      alert('Invalid email or password. Please try again.');
  }
}

const form = document.querySelector('form');
const resultDiv = document.querySelector('#result');

form.addEventListener('submit', (event) => {
  event.preventDefault();

  const reviewerId = document.querySelector('#reviewer-id').value;

  // Create a new FormData object and append the reviewer ID
  const formData = new FormData();
  formData.append('reviewer-id', reviewerId);

  // Make a POST request to the result page URL
  fetch('/result', {
      method: 'POST',
      body: formData
  })
  .then(response => response.json())
  .then(data => {
      // Clear the form and resultDiv
      form.reset();
      resultDiv.innerHTML = '';

      // Display the top 5 ASINs if available
      if (data.top_5_asin) {
          const table = document.createElement('table');
          const thead = document.createElement('thead');
          const tbody = document.createElement('tbody');
          const headerRow = document.createElement('tr');
          const header = document.createElement('th');
          header.textContent = "ASIN Index";
          headerRow.appendChild(header);
          thead.appendChild(headerRow);
          table.appendChild(thead);
  
          data.top_5_asin.forEach(asin => {
              const row = document.createElement('tr');
              const cell = document.createElement('td');
              cell.textContent = asin.asin_index;
              row.appendChild(cell);
              tbody.appendChild(row);
          });
  
          table.appendChild(tbody);
          resultDiv.appendChild(table);
      }
  })
  .catch(error => {
      console.error('Error:', error);
  });
});
