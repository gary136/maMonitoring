

// $(document).ready(function() {
//   $('#search-btn').click(function() {
//     const searchBy = $('#search-by').val();
//     const searchValue = $('#search-input').val();

//     if (searchValue.trim() === '') {
//       alert('Please enter a search value');
//       return;
//     }

//     const url = `/stocks_search?${searchBy}=${searchValue}`;

//     console.log(searchBy + " " + searchValue + " " + url);

//     $.get(url, function(data) {
//         console.log(data);
//       let resultHtml = '';
//       if (data.length === 0) {
//         resultHtml = '<p>No results found.</p>';
//       } else {
//         resultHtml = '<ul>';
//         data.forEach(function(stock) {
//           resultHtml += `<li>
//             Stock Code: ${stock.stock_code}<br>
//             Stock Date: ${stock.stock_date}<br>
//             Closing Price: ${stock.closing_price}<br>
//           </li>`;
//         });
//         resultHtml += '</ul>';
//       }
//       $('#search-results').html(resultHtml);
//     });
//   });
// });

$(document).ready(function() {
  $('#search-btn').click(function() {
    const searchBy = $('#search-by').val();
    const searchValue = $('#search-input').val();
    const searchDirection = $('#search-direction').val(); // Add search direction

    if (searchValue.trim() === '') {
      alert('Please enter a search value');
      return;
    }

    const url = `/stocks_ma_search?search-by=${searchBy}&search-direction=${searchDirection}&search-input=${searchValue}`; // Include search direction in the URL

    console.log(searchBy + " " + searchDirection + " " + searchValue + " " + url);

    // $.get(url, function(data) {
    //   // console.log(data);
    //   let resultHtml = '';
    //   if (data.length === 0) {
    //     resultHtml = '<p>No results found.</p>';
    //   } else {
    //     resultHtml = '<ul>';
    //     data.forEach(function(stock) {
    //       resultHtml += `<li>
    //         Stock Code: ${stock.stock_code}<br>
    //         Closing Price: ${stock.closing_price}<br>
    //         MA Price: ${stock.average_price}<br>
    //       </li>`;
    //     });
    //     resultHtml += '</ul>';
    //   }
    //   $('#search-results').html(resultHtml);
    // });

    $.get(url, function(data) {
      let resultHtml = '';
      if (data.length === 0) {
        resultHtml = '<p>No results found.</p>';
      } else {
        resultHtml = '<div class="stock-codes"><span>Search Results: </span>';
        let stockCodes = '';
        data.forEach(function(stock) {
          stockCodes += `<button class="stock-code" data-code="${stock.stock_code}">${stock.stock_code}</button> | `;
        });
        // Remove the trailing " | "
        stockCodes = stockCodes.slice(0, -3);
        resultHtml += stockCodes;
        resultHtml += '</div>';
      }
      $('#search-results').html(resultHtml);
    
      // Create a container for the iframe
      const iframeContainer = $('<div class="iframe-container"></div>');
      $('#search-results').append(iframeContainer);
    
      // Add click event listener to stock codes
      $('.stock-code').on('click', function() {
        const stockCode = $(this).data('code');
        const chartUrl = `https://www.cmoney.tw/finance/${stockCode}/f00025`;
        const iframeHtml = `<iframe src="${chartUrl}" width="100%" height="500"></iframe>`;
        iframeContainer.html(iframeHtml);
      });
    });
  });
});
