package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);

            try (var database = new OrdersDatabase()) {
                if (database.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

                    System.out.println("new order sent successfully");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("new order sent");
                } else {
                    System.out.println("old order received");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("old order received");
                }
            }


        } catch (InterruptedException ex) {
            throw new ServletException(ex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
