package fr.tictak.dema.service;

import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import fr.tictak.dema.model.MoveRequest;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class PdfService {

    private static final Logger logger = LoggerFactory.getLogger(PdfService.class);

    private final TemplateEngine templateEngine;
    private final ResourceLoader resourceLoader;

    /**
     * Generate PDF from MoveRequest data for the specified template
     * @param moveRequest the move request data
     * @param templateName the name of the template to use (e.g., "devis" or "facture")
     * @return byte array of the generated PDF
     * @throws IOException if PDF generation fails
     */
    public byte[] generatePdf(MoveRequest moveRequest, String templateName) throws IOException {
        logger.info("Starting PDF generation for moveId: {}, template: {}", moveRequest.getMoveId(), templateName);

        try {
            // Create Thymeleaf context with move request data
            Context context = new Context(Locale.FRENCH);
            context.setVariable("moveRequest", moveRequest);

            // Load CSS content
            Resource cssResource = resourceLoader.getResource("classpath:static/css/devis.css");
            if (!cssResource.exists()) {
                logger.error("CSS file not found at classpath:static/css/devis.css");
                throw new IOException("CSS file not found");
            }
            String cssContent = new String(cssResource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            // Process the template to HTML string
            String htmlContent = templateEngine.process(templateName, context);
            logger.debug("HTML template processed successfully for moveId: {}, template: {}", moveRequest.getMoveId(), templateName);

            // Embed CSS into HTML
            String modifiedHtmlContent = htmlContent.replace(
                    "<link rel=\"stylesheet\" href=\"/css/devis.css\"/>",
                    "<style>" + cssContent + "</style>"
            );

            // Convert HTML to PDF
            byte[] pdfBytes = convertHtmlToPdf(modifiedHtmlContent);
            logger.info("PDF generated successfully for moveId: {}, template: {}, size: {} bytes",
                    moveRequest.getMoveId(), templateName, pdfBytes.length);

            return pdfBytes;

        } catch (Exception e) {
            logger.error("Failed to generate PDF for moveId: {}, template: {}", moveRequest.getMoveId(), templateName, e);
            throw new IOException("Failed to generate PDF: " + e.getMessage(), e);
        }
    }

    /**
     * Convert HTML string to PDF bytes using OpenHTMLToPDF
     * @param htmlContent the HTML content to convert
     * @return byte array of the PDF
     * @throws IOException if conversion fails
     */
    private byte[] convertHtmlToPdf(String htmlContent) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            PdfRendererBuilder builder = new PdfRendererBuilder();
            builder.withHtmlContent(htmlContent, null);
            // Specify font to avoid scanning system fonts
            builder.useFont(() -> PdfService.class.getResourceAsStream("/static/fonts/montserrat.ttf"),
                    "montserrat", 400, PdfRendererBuilder.FontStyle.NORMAL, true);
            builder.toStream(outputStream);
            builder.run();

            return outputStream.toByteArray();

        } catch (Exception e) {
            logger.error("Failed to convert HTML to PDF", e);
            throw new IOException("HTML to PDF conversion failed: " + e.getMessage(), e);
        }
    }

    /**
     * Generate filename for the PDF based on move request data and document type
     * @param moveRequest the move request
     * @param documentType the type of document ("Devis" or "Facture")
     * @return formatted filename
     */
    public String generateFilename(MoveRequest moveRequest, String documentType) {
        String clientName = "";
        if (moveRequest.getClient() != null) {
            clientName = (moveRequest.getClient().getFirstName() + "_" +
                    moveRequest.getClient().getLastName()).replaceAll("[^a-zA-Z0-9]", "_");
        }

        String moveId = moveRequest.getMoveId();
        return String.format("TicTak_%s_%s_%s.pdf", documentType, moveId, clientName);
    }
}